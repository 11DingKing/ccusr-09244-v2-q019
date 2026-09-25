"""业务接口与发件箱端到端集成测试。

验证数据集发布、版本撤回、标注批准三个业务动作在请求事务中
登记稳定版本、已脱敏的事件，并能被进程内派发器投递到分析投影。
"""

import pytest
from fastapi.testclient import TestClient

import main
from app.database import get_db
from app.events import build_dispatcher
from app.events.contract import (
    EVENT_ANNOTATION_APPROVED,
    EVENT_DATASET_PUBLISHED,
    EVENT_DATASET_VERSION_REVOKED,
)
from app.models import AnalysisEventLog, OutboxEvent

API = main.api_prefix


@pytest.fixture()
def client(session_factory):
    def _get_db():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    main.app.dependency_overrides[get_db] = _get_db
    # 不使用 with 触发 lifespan：后台派发器在测试中由手动驱动
    with TestClient(main.app, raise_server_exceptions=True) as test_client:
        yield test_client
    main.app.dependency_overrides.clear()


def _create_catalog(client):
    robot = client.post(
        f"{API}/robot-models", json={"name": "机型甲", "manufacturer": "厂商A"}
    ).json()
    scene = client.post(
        f"{API}/scenes", json={"name": "装配工位", "category": "生产制造"}
    ).json()
    skill = client.post(
        f"{API}/skills", json={"name": "精密装配", "category": "装配"}
    ).json()
    return robot["id"], scene["id"], skill["id"]


def _create_operation(client, robot_id, scene_id, skill_id):
    payload = {
        "robot_model_id": robot_id,
        "scene_id": scene_id,
        "skill_id": skill_id,
        "robot_serial": "SN-001",
        "motion_trajectory": {"points": 32},
        "perception_records": {"frames": 10},
        "timestamp_start": "2026-01-01T08:00:00+00:00",
        "timestamp_end": "2026-01-01T08:05:00+00:00",
    }
    return client.post(f"{API}/operations", json=payload).json()["id"]


def _create_dataset_with_item(client, robot_id, scene_id, op_id):
    payload = {
        "name": "首批装配数据",
        "description": "用于分析",
        "robot_model_id": robot_id,
        "scene_id": scene_id,
        "owner_team": "数据平台组",
        "contact_person": "张三私人电话不应外泄",
        "operation_data_ids": [op_id],
    }
    return client.post(f"{API}/datasets", json=payload).json()["id"]


def _outbox_events(client, event_type):
    resp = client.get(f"{API}/outbox/events", params={"event_type": event_type})
    assert resp.status_code == 200
    return resp.json()


def test_dataset_publish_records_versioned_sanitized_event(client, session_factory):
    robot_id, scene_id, _ = _create_catalog(client)
    op_id = _create_operation(client, robot_id, scene_id, 1)
    dataset_id = _create_dataset_with_item(client, robot_id, scene_id, op_id)

    # 订阅方含联系人，事件载荷不得带出
    sub = client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "分析团队", "contact_person": "李四的邮箱"},
    )
    assert sub.status_code == 200

    assert client.post(
        f"{API}/datasets/{dataset_id}/review", json={"action": "submit"}
    ).status_code == 200
    approve = client.post(
        f"{API}/datasets/{dataset_id}/review",
        json={"action": "approve", "reviewer": "审核员王五", "review_notes": "通过"},
    )
    assert approve.status_code == 200

    events = _outbox_events(client, EVENT_DATASET_PUBLISHED)
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "pending"
    assert event["payload_version"] == "1.0"
    assert event["aggregate_type"] == "dataset"
    assert event["aggregate_id"] == str(dataset_id)

    payload = event["payload"]
    assert payload["dataset_id"] == dataset_id
    assert payload["version_label"] == "1.0"
    assert payload["owner_team"] == "数据平台组"
    assert payload["total_items"] == 1
    assert "published_at" in payload
    # 敏感字段绝不进入载荷
    leaked = {"contact_person", "reviewer", "review_notes", "subscriber_team"}
    assert leaked.isdisjoint(payload.keys())

    # 派发器投递到分析投影
    dispatcher = build_dispatcher(session_factory)
    summary = dispatcher.process_batch()
    assert summary.succeeded >= 1

    db = session_factory()
    try:
        row = db.get(OutboxEvent, event["id"])
        assert row.status == "delivered"
        projection = (
            db.query(AnalysisEventLog)
            .filter(AnalysisEventLog.event_id == event["id"])
            .one()
        )
        assert projection.payload_version == "1.0"
        assert "contact_person" not in projection.payload
    finally:
        db.close()


def test_version_revoke_records_event_in_same_transaction(client):
    robot_id, scene_id, _ = _create_catalog(client)
    op_id = _create_operation(client, robot_id, scene_id, 1)
    dataset_id = _create_dataset_with_item(client, robot_id, scene_id, op_id)

    client.post(f"{API}/datasets/{dataset_id}/review", json={"action": "submit"})
    client.post(f"{API}/datasets/{dataset_id}/review", json={"action": "approve"})

    revoke = client.post(
        f"{API}/datasets/{dataset_id}/review", json={"action": "revoke"}
    )
    assert revoke.status_code == 200

    events = _outbox_events(client, EVENT_DATASET_VERSION_REVOKED)
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["dataset_id"] == dataset_id
    assert payload["version_label"] == "1.0"
    assert "revoked_at" in payload

    # 撤回后数据集已下线
    detail = client.get(f"{API}/datasets/{dataset_id}").json()
    assert detail["is_published"] is False
    assert detail["review_status"] == "draft"


def test_failed_state_transition_writes_no_event(client):
    robot_id, scene_id, _ = _create_catalog(client)
    op_id = _create_operation(client, robot_id, scene_id, 1)
    dataset_id = _create_dataset_with_item(client, robot_id, scene_id, op_id)

    # draft 状态不能直接 approve
    resp = client.post(
        f"{API}/datasets/{dataset_id}/review", json={"action": "approve"}
    )
    assert resp.status_code == 400
    assert _outbox_events(client, EVENT_DATASET_PUBLISHED) == []


def test_annotation_approval_records_sanitized_event(client, session_factory):
    robot_id, scene_id, skill_id = _create_catalog(client)
    op_id = _create_operation(client, robot_id, scene_id, skill_id)

    annotation = client.post(
        f"{API}/annotations",
        json={
            "operation_data_id": op_id,
            "is_success": False,
            "failure_category": "感知异常",
            "failure_subcategory": "视觉识别失败",
            "failure_description": "包含现场人员姓名的自由文本",
            "annotator": "标注员赵六",
        },
    ).json()

    approve = client.post(
        f"{API}/annotations/{annotation['id']}/approve",
        json={"reviewer": "审核员钱七", "review_notes": "确认"},
    )
    assert approve.status_code == 200
    assert approve.json()["review_status"] == "approved"

    events = _outbox_events(client, EVENT_ANNOTATION_APPROVED)
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["annotation_id"] == annotation["id"]
    assert payload["operation_data_id"] == op_id
    assert payload["is_success"] is False
    assert payload["failure_category"] == "感知异常"
    assert payload["failure_subcategory"] == "视觉识别失败"
    # 人名与自由文本描述不外泄
    leaked = {"annotator", "reviewer", "review_notes", "failure_description"}
    assert leaked.isdisjoint(payload.keys())

    # 重复批准被拒绝，不会产生第二条事件
    again = client.post(
        f"{API}/annotations/{annotation['id']}/approve", json={}
    )
    assert again.status_code == 400
    assert len(_outbox_events(client, EVENT_ANNOTATION_APPROVED)) == 1

    dispatcher = build_dispatcher(session_factory)
    assert dispatcher.process_batch().succeeded == 1


def test_restart_recovers_pending_events_across_http_and_dispatch(client, session_factory):
    """事件由请求事务产生，派发器在“重启”后仍能消费全部积压。"""
    robot_id, scene_id, skill_id = _create_catalog(client)
    op_id = _create_operation(client, robot_id, scene_id, skill_id)

    produced = []
    for i in range(3):
        dataset_id = client.post(
            f"{API}/datasets",
            json={
                "name": f"数据集{i}",
                "robot_model_id": robot_id,
                "scene_id": scene_id,
                "owner_team": "组",
                "operation_data_ids": [op_id],
            },
        ).json()["id"]
        client.post(f"{API}/datasets/{dataset_id}/review", json={"action": "submit"})
        client.post(f"{API}/datasets/{dataset_id}/review", json={"action": "approve"})
        produced.append(dataset_id)

    pending = client.get(f"{API}/outbox/events", params={"status": "pending"}).json()
    assert len(pending) == 3

    # 模拟新进程启动一个全新派发器实例
    fresh_dispatcher = build_dispatcher(session_factory, batch_size=10)
    fresh_dispatcher.process_batch()

    db = session_factory()
    try:
        assert (
            db.query(OutboxEvent).filter(OutboxEvent.status == "delivered").count()
            == len(produced)
        )
        assert db.query(AnalysisEventLog).count() == len(produced)
    finally:
        db.close()
