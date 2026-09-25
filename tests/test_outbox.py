"""发件箱与派发器测试：提交回滚、并发领取、确认丢失、重试恢复、服务重启。"""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from app.models import (
    AnalyticsProjection,
    Annotation,
    ConsumedEvent,
    Dataset,
    OutboxAttempt,
    OutboxEvent,
)
from app.services.analytics_consumer import AnalyticsConsumer
from app.services.dispatcher import (
    EventConsumer,
    EventMessage,
    OutboxDispatcher,
    exponential_backoff,
)
from app.services.outbox import (
    ANNOTATION_APPROVED,
    DATASET_PUBLISHED,
    DATASET_VERSION_REVOKED,
    PAYLOAD_VERSIONS,
    SENSITIVE_FIELD_NAMES,
    as_utc_naive,
    build_annotation_approved_payload,
    build_dataset_published_payload,
    build_version_revoked_payload,
    record_event,
)

UTC = timezone.utc
T0 = datetime(2026, 1, 1, 8, 0, 0, tzinfo=UTC)


class FakeClock:
    """可注入的测试时钟。"""

    def __init__(self, start=T0):
        self._now = start

    def __call__(self):
        return self._now

    def advance(self, **kwargs):
        self._now += timedelta(**kwargs)


class RecordingConsumer(EventConsumer):
    """记录收到的事件；可配置前几次调用抛异常。"""

    name = "recording"

    def __init__(self, fail_times=0):
        self.messages = []
        self._fail_times = fail_times

    def consume(self, message: EventMessage) -> None:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("模拟消费失败")
        self.messages.append(message)


class AlwaysFailConsumer(EventConsumer):
    name = "always-fail"

    def consume(self, message: EventMessage) -> None:
        raise RuntimeError("持续失败")


def _publish_event(db, clock, max_attempts=5, event_type=DATASET_PUBLISHED, payload=None):
    event = record_event(
        db,
        event_type,
        payload if payload is not None else {"dataset_id": 1, "dataset_name": "演示"},
        clock=clock,
        max_attempts=max_attempts,
    )
    db.commit()
    db.refresh(event)
    db.expunge(event)  # 脱离会话返回，避免 commit 过期后在会话外访问属性
    return event


def _get_event(session_factory, event_id):
    with session_factory() as db:
        event = db.query(OutboxEvent).filter(OutboxEvent.event_id == event_id).first()
        if event is not None:
            db.expunge(event)
        return event


def _attempts(session_factory, event_id):
    with session_factory() as db:
        attempts = (
            db.query(OutboxAttempt)
            .filter(OutboxAttempt.event_id == event_id)
            .order_by(OutboxAttempt.attempt_number)
            .all()
        )
        db.expunge_all()
        return attempts


# ---------------------------------------------------------------------------
# 提交与回滚：业务写入与事件记录同事务
# ---------------------------------------------------------------------------

def test_event_commits_with_business_transaction(db):
    clock = FakeClock()
    event = record_event(db, DATASET_PUBLISHED, {"dataset_id": 7}, clock=clock)
    db.commit()

    stored = db.query(OutboxEvent).filter(OutboxEvent.event_id == event.event_id).one()
    assert stored.status == OutboxEvent.STATUS_PENDING
    assert stored.payload_version == PAYLOAD_VERSIONS[DATASET_PUBLISHED]
    assert stored.available_at == as_utc_naive(T0)
    assert stored.attempts == 0


def test_event_rolls_back_with_business_transaction(db):
    event = record_event(db, DATASET_PUBLISHED, {"dataset_id": 7}, clock=FakeClock())
    db.rollback()

    assert db.query(OutboxEvent).filter(OutboxEvent.event_id == event.event_id).count() == 0


def test_record_event_rejects_unknown_type_and_sensitive_fields(db):
    with pytest.raises(ValueError):
        record_event(db, "unknown.event", {})
    with pytest.raises(ValueError):
        record_event(db, DATASET_PUBLISHED, {"dataset_id": 1, "contact_person": "张三"})
    db.rollback()


# ---------------------------------------------------------------------------
# 载荷：稳定版本 + 敏感字段不外泄
# ---------------------------------------------------------------------------

def test_payload_builders_use_stable_version_and_hide_sensitive_fields(db):
    dataset = Dataset(
        name="焊接数据集", owner_team="数据组", contact_person="张三",
        robot_model_id=1, scene_id=1, current_version=3, version="1.2",
        published_at=datetime(2026, 2, 1, tzinfo=UTC),
    )
    annotation = Annotation(
        operation_data_id=9, is_success=False, failure_category="感知异常",
        annotator="李四", reviewer="王五", review_notes="内部备注",
    )

    published = build_dataset_published_payload(
        dataset, version_number=3, version_label="1.2", subscriber_teams=["分析组"]
    )
    revoked = build_version_revoked_payload(dataset, "revoke")
    approved = build_annotation_approved_payload(annotation)

    for payload in (published, revoked, approved):
        assert SENSITIVE_FIELD_NAMES.isdisjoint(payload)
        assert "张三" not in payload.values()
        assert "李四" not in payload.values()
        assert "王五" not in payload.values()

    assert published["version_number"] == 3
    assert published["subscriber_teams"] == ["分析组"]
    assert revoked["reason"] == "revoke"
    assert approved["operation_data_id"] == 9
    assert set(PAYLOAD_VERSIONS) == {DATASET_PUBLISHED, DATASET_VERSION_REVOKED, ANNOTATION_APPROVED}


# ---------------------------------------------------------------------------
# 领取：顺序与并发安全
# ---------------------------------------------------------------------------

def test_dispatch_delivers_events_in_creation_order(session_factory):
    clock = FakeClock()
    with session_factory() as db:
        ids = [_publish_event(db, clock).event_id for _ in range(5)]

    consumer = RecordingConsumer()
    dispatcher = OutboxDispatcher(session_factory, consumer, clock=clock)
    report = dispatcher.dispatch_pending()

    assert report.delivered == 5
    assert [m.event_id for m in consumer.messages] == ids
    with session_factory() as db:
        for event_id in ids:
            event = db.query(OutboxEvent).filter(OutboxEvent.event_id == event_id).one()
            assert event.status == OutboxEvent.STATUS_DELIVERED
            assert event.delivered_at == as_utc_naive(T0)
            assert event.attempts == 1


def test_concurrent_claim_assigns_each_event_to_exactly_one_worker(session_factory):
    clock = FakeClock()
    with session_factory() as db:
        expected = {_publish_event(db, clock).id for _ in range(40)}

    dispatcher_a = OutboxDispatcher(session_factory, RecordingConsumer(), clock=clock, worker_id="worker-a")
    dispatcher_b = OutboxDispatcher(session_factory, RecordingConsumer(), clock=clock, worker_id="worker-b")

    # 屏障让两个 worker 尽量同时开始领取，制造真实竞争
    barrier = threading.Barrier(2)

    def claim(dispatcher):
        barrier.wait(timeout=10)
        return dispatcher.claim_batch()

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(claim, dispatcher_a)
        future_b = pool.submit(claim, dispatcher_b)
        claimed_a, claimed_b = set(future_a.result()), set(future_b.result())

    # 互斥性：同一事件不会被两个 worker 同时领取；完备性：所有事件都被领走
    assert claimed_a.isdisjoint(claimed_b)
    assert claimed_a | claimed_b == expected

    # 数据库中的归属与领取返回值一致
    with session_factory() as db:
        events = db.query(OutboxEvent).all()
        assert len(events) == len(expected)
        for event in events:
            assert event.status == OutboxEvent.STATUS_CLAIMED
            if event.id in claimed_a:
                assert event.claimed_by == "worker-a"
            else:
                assert event.claimed_by == "worker-b"


def test_concurrent_dispatch_delivers_each_event_exactly_once(session_factory):
    clock = FakeClock()
    with session_factory() as db:
        expected = {_publish_event(db, clock).event_id for _ in range(20)}

    consumer = RecordingConsumer()  # 两个线程共享，list.append 线程安全
    dispatcher_a = OutboxDispatcher(session_factory, consumer, clock=clock, worker_id="worker-a")
    dispatcher_b = OutboxDispatcher(session_factory, consumer, clock=clock, worker_id="worker-b")

    barrier = threading.Barrier(2)

    def dispatch(dispatcher):
        barrier.wait(timeout=10)
        return dispatcher.dispatch_pending()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [f.result() for f in [pool.submit(dispatch, d) for d in (dispatcher_a, dispatcher_b)]]

    consumed_ids = [m.event_id for m in consumer.messages]
    assert len(consumed_ids) == len(set(consumed_ids))  # 无重复消费
    assert set(consumed_ids) == expected
    assert sum(r.delivered for r in results) == len(expected)


# ---------------------------------------------------------------------------
# 确认丢失：消费确认已保存但事件未标记，重派不产生二次影响
# ---------------------------------------------------------------------------

def test_redelivery_after_lost_ack_has_no_duplicate_effect(session_factory):
    clock = FakeClock()
    with session_factory() as db:
        event = _publish_event(db, clock)
        message = EventMessage(
            event_id=event.event_id,
            event_type=event.event_type,
            payload_version=event.payload_version,
            payload=dict(event.payload),
            attempts=1,
            created_at=event.created_at,
        )

    consumer = AnalyticsConsumer(session_factory, clock=clock)
    # 模拟：消费者已提交副作用与确认，随后进程崩溃，事件仍是 pending（确认结果丢失）
    consumer.consume(message)

    with session_factory() as db:
        projection = db.query(AnalyticsProjection).filter_by(metric=f"{DATASET_PUBLISHED}.count").one()
        assert projection.value == 1
        assert db.query(ConsumedEvent).count() == 1
        stored = db.query(OutboxEvent).filter(OutboxEvent.event_id == event.event_id).one()
        assert stored.status == OutboxEvent.STATUS_PENDING

    # 重启后重派：消费者幂等跳过，事件补标 delivered，投影不重复计数
    dispatcher = OutboxDispatcher(session_factory, consumer, clock=clock, worker_id="worker-restarted")
    report = dispatcher.dispatch_pending()

    assert report.delivered == 1
    with session_factory() as db:
        projection = db.query(AnalyticsProjection).filter_by(metric=f"{DATASET_PUBLISHED}.count").one()
        assert projection.value == 1
        assert db.query(ConsumedEvent).count() == 1
        stored = db.query(OutboxEvent).filter(OutboxEvent.event_id == event.event_id).one()
        assert stored.status == OutboxEvent.STATUS_DELIVERED


def test_duplicate_concurrent_ack_is_idempotent(session_factory):
    clock = FakeClock()
    with session_factory() as db:
        event = _publish_event(db, clock)
        message = EventMessage(
            event_id=event.event_id, event_type=event.event_type,
            payload_version=event.payload_version, payload=dict(event.payload),
            attempts=1, created_at=event.created_at,
        )

    consumer = AnalyticsConsumer(session_factory, clock=clock)
    consumer.consume(message)
    consumer.consume(message)  # 同一事件重复确认

    with session_factory() as db:
        projection = db.query(AnalyticsProjection).filter_by(metric=f"{DATASET_PUBLISHED}.count").one()
        assert projection.value == 1
        assert db.query(ConsumedEvent).count() == 1


# ---------------------------------------------------------------------------
# 重试：按注入时钟计算重试时间，恢复后成功
# ---------------------------------------------------------------------------

def test_failed_event_retries_after_backoff_and_recovers(session_factory):
    clock = FakeClock()
    backoff = exponential_backoff(base_seconds=10, factor=2)
    with session_factory() as db:
        event = _publish_event(db, clock)

    consumer = RecordingConsumer(fail_times=1)
    dispatcher = OutboxDispatcher(
        session_factory, consumer, clock=clock, retry_policy=backoff
    )

    report = dispatcher.dispatch_pending()
    assert report.retried == 1 and report.delivered == 0

    stored = _get_event(session_factory, event.event_id)
    assert stored.status == OutboxEvent.STATUS_PENDING
    assert stored.attempts == 1
    assert stored.last_error == "模拟消费失败"
    # 重试时间由注入时钟计算：t0 + backoff(1)
    assert stored.available_at == as_utc_naive(T0) + backoff(1)

    # 时钟未走到重试时间：不会再被领取
    assert dispatcher.dispatch_pending().claimed == 0

    # 时钟推进后重试成功
    clock.advance(seconds=11)
    report = dispatcher.dispatch_pending()
    assert report.delivered == 1
    assert [m.event_id for m in consumer.messages] == [event.event_id]

    stored = _get_event(session_factory, event.event_id)
    assert stored.status == OutboxEvent.STATUS_DELIVERED
    assert stored.attempts == 2

    attempts = _attempts(session_factory, event.event_id)
    assert [a.outcome for a in attempts] == ["failure", "success"]
    assert [a.attempt_number for a in attempts] == [1, 2]


def test_event_quarantined_after_max_attempts_and_stops_retrying(session_factory):
    clock = FakeClock()
    backoff = exponential_backoff(base_seconds=5)
    with session_factory() as db:
        event = _publish_event(db, clock, max_attempts=3)

    dispatcher = OutboxDispatcher(
        session_factory, AlwaysFailConsumer(), clock=clock, retry_policy=backoff
    )

    assert dispatcher.dispatch_pending().retried == 1
    clock.advance(seconds=10)
    assert dispatcher.dispatch_pending().retried == 1
    clock.advance(seconds=20)
    report = dispatcher.dispatch_pending()
    assert report.quarantined == 1

    stored = _get_event(session_factory, event.event_id)
    assert stored.status == OutboxEvent.STATUS_QUARANTINED
    assert stored.attempts == 3

    # 隔离后不再被领取
    clock.advance(hours=1)
    assert dispatcher.dispatch_pending().claimed == 0

    attempts = _attempts(session_factory, event.event_id)
    assert len(attempts) == 3
    assert all(a.outcome == "failure" for a in attempts)


# ---------------------------------------------------------------------------
# 服务重启：待派发事件不丢失，卡死的领取被回收
# ---------------------------------------------------------------------------

def test_pending_events_survive_dispatcher_restart(session_factory):
    clock = FakeClock()
    with session_factory() as db:
        ids = [_publish_event(db, clock).event_id for _ in range(3)]

    # “重启”：同一数据库上构造全新的派发器实例
    consumer = RecordingConsumer()
    dispatcher = OutboxDispatcher(session_factory, consumer, clock=clock, worker_id="worker-new")
    report = dispatcher.dispatch_pending()

    assert report.delivered == 3
    assert [m.event_id for m in consumer.messages] == ids


def test_stale_claim_is_recovered_after_crash(session_factory):
    clock = FakeClock()
    with session_factory() as db:
        event = _publish_event(db, clock)

    # worker-a 领取后“崩溃”：事件停留在 claimed 状态
    crashed = OutboxDispatcher(session_factory, RecordingConsumer(), clock=clock, worker_id="worker-a")
    assert crashed.claim_batch() == [event.id]

    # 新实例在租约过期后回收并派发
    clock.advance(seconds=301)
    consumer = RecordingConsumer()
    restarted = OutboxDispatcher(
        session_factory, consumer, clock=clock, worker_id="worker-b", lease_seconds=300
    )
    report = restarted.dispatch_pending()

    assert report.recovered == 1
    assert report.delivered == 1
    assert [m.event_id for m in consumer.messages] == [event.event_id]

    stored = _get_event(session_factory, event.event_id)
    assert stored.status == OutboxEvent.STATUS_DELIVERED


# ---------------------------------------------------------------------------
# HTTP 集成：业务操作产生事件，失败请求不产生事件，隔离状态可查询
# ---------------------------------------------------------------------------

def _create_operation(client, robot_model_id, scene_id, skill_id):
    response = client.post("/api/v1/operations", json={
        "robot_model_id": robot_model_id,
        "scene_id": scene_id,
        "skill_id": skill_id,
        "motion_trajectory": {"points": [[0, 0, 0]]},
        "perception_records": {"frames": 10},
        "timestamp_start": "2026-01-01T00:00:00Z",
        "timestamp_end": "2026-01-01T00:01:00Z",
    })
    assert response.status_code == 200, response.text
    return response.json()


def _create_dataset_with_item(client):
    rm = client.post("/api/v1/robot-models", json={"name": "RM-1", "manufacturer": "ACME"}).json()
    scene = client.post("/api/v1/scenes", json={"name": "总装车间", "category": "制造"}).json()
    skill = client.post("/api/v1/skills", json={"name": "抓取", "category": "操作"}).json()
    op = _create_operation(client, rm["id"], scene["id"], skill["id"])
    response = client.post("/api/v1/datasets", json={
        "name": "抓取数据集",
        "robot_model_id": rm["id"],
        "scene_id": scene["id"],
        "skill_id": skill["id"],
        "owner_team": "数据组",
        "contact_person": "张三",
        "operation_data_ids": [op["id"]],
    })
    assert response.status_code == 200, response.text
    return response.json(), op


def _outbox_events(client, **params):
    response = client.get("/api/v1/outbox/events", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_dataset_approve_and_revoke_emit_outbox_events(client):
    dataset, _ = _create_dataset_with_item(client)
    client.post(
        f"/api/v1/datasets/{dataset['id']}/subscriptions",
        json={"subscriber_team": "分析组", "contact_person": "李四"},
    )
    assert client.post(f"/api/v1/datasets/{dataset['id']}/review", json={"action": "submit"}).status_code == 200
    assert client.post(
        f"/api/v1/datasets/{dataset['id']}/review",
        json={"action": "approve", "reviewer": "王五", "review_notes": "同意发布"},
    ).status_code == 200

    published = _outbox_events(client, event_type=DATASET_PUBLISHED)
    assert len(published) == 1
    payload = published[0]["payload"]
    assert published[0]["payload_version"] == 1
    assert published[0]["status"] == "pending"
    assert payload["dataset_id"] == dataset["id"]
    assert payload["subscriber_teams"] == ["分析组"]
    # 敏感字段（联系人、审核人、审核意见）不进入载荷
    assert SENSITIVE_FIELD_NAMES.isdisjoint(payload)

    assert client.post(f"/api/v1/datasets/{dataset['id']}/review", json={"action": "revoke"}).status_code == 200
    revoked = _outbox_events(client, event_type=DATASET_VERSION_REVOKED)
    assert len(revoked) == 1
    assert revoked[0]["payload"]["reason"] == "revoke"
    assert SENSITIVE_FIELD_NAMES.isdisjoint(revoked[0]["payload"])


def test_failed_business_request_leaves_no_event(client):
    dataset, _ = _create_dataset_with_item(client)
    # 未提交审核直接批准：业务校验失败，整个事务不应留下事件
    response = client.post(f"/api/v1/datasets/{dataset['id']}/review", json={"action": "approve"})
    assert response.status_code == 400
    assert _outbox_events(client) == []


def test_annotation_approval_emits_outbox_event(client):
    _, op = _create_dataset_with_item(client)
    annotation = client.post("/api/v1/annotations", json={
        "operation_data_id": op["id"],
        "is_success": False,
        "failure_category": "感知异常",
        "annotator": "张三",
    }).json()

    response = client.put(
        f"/api/v1/annotations/{annotation['id']}",
        json={"review_status": "approved", "reviewer": "王五", "review_notes": "标注准确"},
    )
    assert response.status_code == 200

    events = _outbox_events(client, event_type=ANNOTATION_APPROVED)
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["annotation_id"] == annotation["id"]
    assert payload["operation_data_id"] == op["id"]
    assert payload["failure_category"] == "感知异常"
    assert SENSITIVE_FIELD_NAMES.isdisjoint(payload)

    # 重复置为 approved（状态未再变化）不产生第二个事件
    client.put(f"/api/v1/annotations/{annotation['id']}", json={"review_status": "approved"})
    assert len(_outbox_events(client, event_type=ANNOTATION_APPROVED)) == 1


def test_manual_dispatch_endpoint_delivers_and_exposes_quarantine(client, session_factory):
    dataset, _ = _create_dataset_with_item(client)
    client.post(f"/api/v1/datasets/{dataset['id']}/review", json={"action": "submit"})
    client.post(f"/api/v1/datasets/{dataset['id']}/review", json={"action": "approve"})

    report = client.post("/api/v1/outbox/dispatch").json()
    assert report["delivered"] == 1

    delivered = _outbox_events(client, status="delivered")
    assert len(delivered) == 1
    event_id = delivered[0]["event_id"]

    attempts = client.get(f"/api/v1/outbox/events/{event_id}/attempts").json()
    assert [a["outcome"] for a in attempts] == ["success"]

    # 投影只计数一次
    with session_factory() as db:
        projection = db.query(AnalyticsProjection).filter_by(metric=f"{DATASET_PUBLISHED}.count").one()
        assert projection.value == 1

    # 制造一个持续失败的事件并观察其进入可查询的隔离状态
    clock = FakeClock()
    with session_factory() as db:
        failing = _publish_event(db, clock, max_attempts=2, event_type=DATASET_VERSION_REVOKED,
                                 payload={"dataset_id": dataset["id"]})
    dispatcher = OutboxDispatcher(
        session_factory, AlwaysFailConsumer(), clock=clock,
        retry_policy=exponential_backoff(base_seconds=1),
    )
    dispatcher.dispatch_pending()
    clock.advance(seconds=10)
    dispatcher.dispatch_pending()

    quarantined = _outbox_events(client, status="quarantined")
    assert [e["event_id"] for e in quarantined] == [failing.event_id]
