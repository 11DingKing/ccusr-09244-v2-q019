"""发件箱管理服务：事件状态查询、尝试留痕、死信重投。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy.orm import Session

from app.models import OutboxDeliveryAttempt, OutboxEvent

STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DELIVERED = "delivered"
STATUS_DEAD_LETTER = "dead_letter"

VALID_STATUSES = {
    STATUS_PENDING,
    STATUS_PROCESSING,
    STATUS_DELIVERED,
    STATUS_DEAD_LETTER,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def list_events(
    db: Session,
    *,
    status: Optional[str] = None,
    event_type: Optional[str] = None,
    aggregate_id: Optional[str] = None,
    limit: int = 100,
) -> List[OutboxEvent]:
    query = db.query(OutboxEvent)
    if status:
        query = query.filter(OutboxEvent.status == status)
    if event_type:
        query = query.filter(OutboxEvent.event_type == event_type)
    if aggregate_id:
        query = query.filter(OutboxEvent.aggregate_id == str(aggregate_id))
    return query.order_by(OutboxEvent.id.desc()).limit(limit).all()


def get_event(db: Session, event_id: int) -> Optional[OutboxEvent]:
    return db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()


def list_attempts(db: Session, event_id: int) -> List[OutboxDeliveryAttempt]:
    return (
        db.query(OutboxDeliveryAttempt)
        .filter(OutboxDeliveryAttempt.event_id == event_id)
        .order_by(OutboxDeliveryAttempt.id)
        .all()
    )


def replay_dead_letter(
    db: Session, event_id: int, *, now: Optional[datetime] = None, reset_attempts: bool = False
) -> OutboxEvent:
    """把死信事件重新放回队列等待派发。

    默认保留已消耗的尝试次数（再次失败仍按退避表延后），
    ``reset_attempts=True`` 时同时清零计数，用于故障修复后
    重新给足完整重试预算。
    """
    event = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
    if event is None:
        raise LookupError(f"事件不存在: {event_id}")
    if event.status != STATUS_DEAD_LETTER:
        raise ValueError(f"仅死信事件可重投，当前状态: {event.status}")

    moment = now or utc_now()
    event.status = STATUS_PENDING
    event.available_at = moment
    event.locked_at = None
    event.locked_by = None
    event.dead_letter_at = None
    event.dead_letter_reason = None
    event.last_error = None
    if reset_attempts:
        event.attempt_count = 0
    db.commit()
    db.refresh(event)
    return event
