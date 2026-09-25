"""发件箱写入：业务代码在自己的事务中登记事件。

``record_event`` 只向当前会话追加 ``OutboxEvent``，不提交事务——
调用方必须让业务写入与事件行落在同一个 ``db.commit()`` 中，
事务回滚时事件随之消失，从根本上避免“状态已变但事件缺失”。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.config import settings
from app.events.contract import payload_version_for, build_payload
from app.models import OutboxEvent


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def record_event(
    db: Session,
    event_type: str,
    aggregate_type: str,
    aggregate_id: Any,
    *,
    max_attempts: Optional[int] = None,
    now: Optional[datetime] = None,
    **payload_fields: Any,
) -> OutboxEvent:
    """在当前事务中登记一条发件箱事件（不提交）。

    :param max_attempts: 最大投递次数，默认取 ``OUTBOX_MAX_ATTEMPTS``。
    :param now: 可注入的当前时间，主要用于测试。
    """
    moment = now or utc_now()
    payload = build_payload(event_type, **payload_fields)

    event = OutboxEvent(
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=str(aggregate_id),
        payload_version=payload_version_for(event_type),
        payload=payload,
        status="pending",
        attempt_count=0,
        max_attempts=settings.OUTBOX_MAX_ATTEMPTS if max_attempts is None else max_attempts,
        available_at=moment,
    )
    db.add(event)
    db.flush()  # 填充 id，但不提交；随调用方事务一起提交/回滚
    return event
