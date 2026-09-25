"""业务事件契约：事件类型、稳定载荷版本与脱敏字段白名单。

内部分析组件消费的业务事件必须满足：

1. ``payload_version`` 显式存在，载荷结构只能向后兼容地演进；
2. 只暴露分析所需的最小字段，联系人、标注人等可识别人员信息
   一律不进入事件载荷。
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

#: 数据集发布（审核通过即发布）
EVENT_DATASET_PUBLISHED = "dataset.published.v1"
#: 已发布版本被撤回
EVENT_DATASET_VERSION_REVOKED = "dataset.version_revoked.v1"
#: 人工标注经审核批准
EVENT_ANNOTATION_APPROVED = "annotation.approved.v1"

#: 当前支持的事件类型集合
KNOWN_EVENT_TYPES = {
    EVENT_DATASET_PUBLISHED,
    EVENT_DATASET_VERSION_REVOKED,
    EVENT_ANNOTATION_APPROVED,
}

#: 每种事件对应的稳定载荷版本。结构只能增加可选字段，
#: 不得删除或改变既有字段语义；不兼容的变更须新建事件类型。
PAYLOAD_VERSIONS: Dict[str, str] = {
    EVENT_DATASET_PUBLISHED: "1.0",
    EVENT_DATASET_VERSION_REVOKED: "1.0",
    EVENT_ANNOTATION_APPROVED: "1.0",
}


class EventPayloadError(ValueError):
    """事件类型未知或参数不完整。"""


# 各事件允许进入载荷的字段白名单（字段 -> 必传）。
# 注意：owner_team 是团队标识，分析侧需要按团队聚合，保留；
# contact_person / reviewer / annotator 等个人信息绝不放入载荷。
_FIELD_WHITELIST: Dict[str, Dict[str, bool]] = {
    EVENT_DATASET_PUBLISHED: {
        "dataset_id": True,
        "dataset_name": True,
        "version_label": True,
        "version_number": True,
        "robot_model_id": True,
        "scene_id": True,
        "skill_id": False,
        "owner_team": True,
        "total_items": True,
        "success_count": True,
        "failure_count": True,
        "annotation_complete_rate": True,
        "average_quality_score": False,
        "data_grade": False,
        "published_at": True,
    },
    EVENT_DATASET_VERSION_REVOKED: {
        "dataset_id": True,
        "dataset_name": True,
        "version_label": True,
        "version_number": True,
        "reused_at_release": False,
        "revoked_at": True,
    },
    EVENT_ANNOTATION_APPROVED: {
        "annotation_id": True,
        "operation_data_id": True,
        "robot_model_id": True,
        "scene_id": True,
        "skill_id": True,
        "is_success": True,
        "failure_category": False,
        "failure_subcategory": False,
        "annotation_quality_score": False,
        "approved_at": True,
    },
}


def build_payload(event_type: str, **fields: Any) -> Dict[str, Any]:
    """按白名单构造稳定版本的事件载荷。

    额外传入的字段会被丢弃而不是报错——业务侧提供再丰富的信息也
    不会意外泄露到事件里；缺失必传字段则抛出 ``EventPayloadError``。
    """
    spec = _FIELD_WHITELIST.get(event_type)
    if spec is None:
        raise EventPayloadError(f"未知事件类型: {event_type}")

    payload: Dict[str, Any] = {}
    for field, required in spec.items():
        if field in fields and fields[field] is not None:
            payload[field] = fields[field]
        elif required:
            raise EventPayloadError(
                f"事件 {event_type} 缺少必传字段: {field}"
            )
    return payload


def payload_version_for(event_type: str) -> str:
    version = PAYLOAD_VERSIONS.get(event_type)
    if version is None:
        raise EventPayloadError(f"未知事件类型: {event_type}")
    return version


def payload_contract(event_type: str) -> Tuple[str, Dict[str, bool]]:
    """返回事件的（载荷版本, 字段规约），供测试与文档使用。"""
    version = payload_version_for(event_type)
    return version, dict(_FIELD_WHITELIST[event_type])
