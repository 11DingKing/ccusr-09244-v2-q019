"""持久化发件箱：业务写入与事件记录在同一事务完成。

事件载荷使用稳定的版本号（``PAYLOAD_VERSIONS``），并通过白名单字段构造，
确保联系人、审核人等敏感字段不会进入事件载荷。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Iterable, Optional
from uuid import uuid4

from sqlalchemy.orm import Session

from app.models import Annotation, Dataset, OutboxEvent

# 业务事件类型
DATASET_PUBLISHED = "dataset.published"
DATASET_VERSION_REVOKED = "dataset.version_revoked"
ANNOTATION_APPROVED = "annotation.approved"

# 每种事件类型的稳定载荷版本
PAYLOAD_VERSIONS = {
    DATASET_PUBLISHED: 1,
    DATASET_VERSION_REVOKED: 1,
    ANNOTATION_APPROVED: 1,
}

# 不允许进入事件载荷的敏感字段名（防御性校验，载荷构造本身已是白名单）
SENSITIVE_FIELD_NAMES = frozenset({
    "contact_person",
    "reviewer",
    "annotator",
    "review_notes",
})

Clock = Callable[[], datetime]


def system_clock() -> datetime:
    return datetime.now(timezone.utc)


def as_utc_naive(moment: datetime) -> datetime:
    """统一转换为 UTC 朴素时间，避免 SQLite 下带时区与朴素时间混用。"""
    if moment.tzinfo is None:
        return moment
    return moment.astimezone(timezone.utc).replace(tzinfo=None)


def _iso(moment: Optional[datetime]) -> Optional[str]:
    if moment is None:
        return None
    return as_utc_naive(moment).isoformat()


def _assert_payload_sanitized(payload: dict) -> None:
    leaked = SENSITIVE_FIELD_NAMES.intersection(payload)
    if leaked:
        raise ValueError(f"事件载荷包含敏感字段: {sorted(leaked)}")


def build_dataset_published_payload(
    dataset: Dataset,
    *,
    version_number: int,
    version_label: str,
    subscriber_teams: Iterable[str] = (),
) -> dict:
    """数据集发布事件载荷（版本 1）。不含联系人等敏感字段。"""
    return {
        "dataset_id": dataset.id,
        "dataset_name": dataset.name,
        "owner_team": dataset.owner_team,
        "version_number": version_number,
        "version_label": version_label,
        "published_at": _iso(dataset.published_at),
        "subscriber_teams": sorted(subscriber_teams),
    }


def build_version_revoked_payload(dataset: Dataset, reason: str) -> dict:
    """版本撤回事件载荷（版本 1）。不含审核人、审核意见等敏感字段。"""
    return {
        "dataset_id": dataset.id,
        "dataset_name": dataset.name,
        "owner_team": dataset.owner_team,
        "version_number": dataset.current_version,
        "version_label": dataset.version,
        "reason": reason,
    }


def build_annotation_approved_payload(annotation: Annotation) -> dict:
    """标注批准事件载荷（版本 1）。不含标注人、审核人等敏感字段。"""
    return {
        "annotation_id": annotation.id,
        "operation_data_id": annotation.operation_data_id,
        "is_success": annotation.is_success,
        "failure_category": annotation.failure_category,
        "approved_at": _iso(as_utc_naive(system_clock())),
    }


def record_event(
    db: Session,
    event_type: str,
    payload: dict,
    *,
    clock: Clock = system_clock,
    max_attempts: int = 5,
) -> OutboxEvent:
    """在当前业务事务内记录一条发件箱事件（调用方负责 commit/rollback）。

    业务写入与事件记录因此保持同生共死：事务回滚时事件一并消失，
    事务提交后事件必然存在，等待派发器领取。
    """
    if event_type not in PAYLOAD_VERSIONS:
        raise ValueError(f"未知的事件类型: {event_type}")
    _assert_payload_sanitized(payload)

    event = OutboxEvent(
        event_id=uuid4().hex,
        event_type=event_type,
        payload_version=PAYLOAD_VERSIONS[event_type],
        payload=payload,
        status=OutboxEvent.STATUS_PENDING,
        attempts=0,
        max_attempts=max_attempts,
        available_at=as_utc_naive(clock()),
    )
    db.add(event)
    db.flush()
    return event
