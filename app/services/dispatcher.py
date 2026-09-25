"""进程内发件箱派发器：按顺序领取事件、调用消费者并记录每次尝试结果。

- 领取（claim）通过带状态条件的原子 UPDATE 完成，并发worker不会领到同一事件；
- 失败事件按可注入时钟计算下次可领取时间（指数退避），超过上限进入隔离状态；
- 领取租约过期的事件会被回收，进程崩溃或重启后不会永久卡死。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, List, Optional
from uuid import uuid4

from sqlalchemy import update
from sqlalchemy.orm import Session, sessionmaker

from app.models import OutboxAttempt, OutboxEvent
from app.services.outbox import Clock, as_utc_naive, system_clock

logger = logging.getLogger(__name__)

DEFAULT_LEASE_SECONDS = 300.0


@dataclass(frozen=True)
class EventMessage:
    """派发给消费者的不可变事件视图，与数据库会话解耦。"""

    event_id: str
    event_type: str
    payload_version: int
    payload: dict
    attempts: int
    created_at: Optional[datetime]


class EventConsumer:
    """消费者协议：处理事件，抛异常表示本次派发失败。"""

    name = "consumer"

    def consume(self, message: EventMessage) -> None:  # pragma: no cover - 接口定义
        raise NotImplementedError


def exponential_backoff(
    base_seconds: float = 1.0,
    factor: float = 2.0,
    max_seconds: float = 300.0,
) -> Callable[[int], timedelta]:
    """生成按尝试次数增长的退避策略（attempt 从 1 开始）。"""

    def policy(attempt: int) -> timedelta:
        seconds = min(base_seconds * (factor ** max(attempt - 1, 0)), max_seconds)
        return timedelta(seconds=seconds)

    return policy


@dataclass
class DispatchReport:
    claimed: int = 0
    delivered: int = 0
    retried: int = 0
    quarantined: int = 0
    recovered: int = 0
    errors: List[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "claimed": self.claimed,
            "delivered": self.delivered,
            "retried": self.retried,
            "quarantined": self.quarantined,
            "recovered": self.recovered,
            "errors": list(self.errors),
        }


class OutboxDispatcher:
    """从发件箱领取事件并派发给消费者的进程内派发器。"""

    def __init__(
        self,
        session_factory: sessionmaker,
        consumer: EventConsumer,
        *,
        clock: Clock = system_clock,
        retry_policy: Optional[Callable[[int], timedelta]] = None,
        worker_id: Optional[str] = None,
        batch_size: int = 50,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = consumer
        self._clock = clock
        self._retry_policy = retry_policy or exponential_backoff()
        self._worker_id = worker_id or f"worker-{uuid4().hex[:8]}"
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def _now(self) -> datetime:
        return as_utc_naive(self._clock())

    # ------------------------------------------------------------------ claim

    def claim_batch(self, limit: Optional[int] = None) -> List[int]:
        """原子领取待派发事件，返回按创建顺序排列的事件主键列表。"""
        now = self._now()
        with self._session_factory() as db:
            rows = (
                db.query(OutboxEvent.id)
                .filter(
                    OutboxEvent.status == OutboxEvent.STATUS_PENDING,
                    OutboxEvent.available_at <= now,
                )
                .order_by(OutboxEvent.id)
                .limit(limit or self._batch_size)
                .all()
            )
            candidate_ids = [row[0] for row in rows]

        claimed: List[int] = []
        for event_pk in candidate_ids:
            # 条件 UPDATE 保证并发 worker 中只有一个能把事件从 pending 置为 claimed
            with self._session_factory() as db:
                result = db.execute(
                    update(OutboxEvent)
                    .where(
                        OutboxEvent.id == event_pk,
                        OutboxEvent.status == OutboxEvent.STATUS_PENDING,
                    )
                    .values(
                        status=OutboxEvent.STATUS_CLAIMED,
                        claimed_by=self._worker_id,
                        claimed_at=now,
                    )
                )
                db.commit()
                if result.rowcount == 1:
                    claimed.append(event_pk)
        return claimed

    def recover_stale_claims(self, lease_seconds: Optional[float] = None) -> int:
        """把领取后超过租约时间仍未完成的事件重置为 pending（崩溃/重启恢复）。"""
        lease = self._lease_seconds if lease_seconds is None else lease_seconds
        cutoff = self._now() - timedelta(seconds=lease)
        with self._session_factory() as db:
            result = db.execute(
                update(OutboxEvent)
                .where(
                    OutboxEvent.status == OutboxEvent.STATUS_CLAIMED,
                    OutboxEvent.claimed_at < cutoff,
                )
                .values(status=OutboxEvent.STATUS_PENDING, claimed_by=None, claimed_at=None)
            )
            db.commit()
            return result.rowcount or 0

    # --------------------------------------------------------------- dispatch

    def dispatch_pending(self) -> DispatchReport:
        """完成一轮：回收过期租约 → 按序领取 → 逐个派发。"""
        report = DispatchReport()
        report.recovered = self.recover_stale_claims()
        event_ids = self.claim_batch()
        report.claimed = len(event_ids)
        for event_pk in event_ids:
            outcome = self._dispatch_one(event_pk)
            if outcome == "delivered":
                report.delivered += 1
            elif outcome == "quarantined":
                report.quarantined += 1
            elif outcome == "retried":
                report.retried += 1
        return report

    def _dispatch_one(self, event_pk: int) -> str:
        with self._session_factory() as db:
            event = db.get(OutboxEvent, event_pk)
            if (
                event is None
                or event.status != OutboxEvent.STATUS_CLAIMED
                or event.claimed_by != self._worker_id
            ):
                return "skipped"
            message = EventMessage(
                event_id=event.event_id,
                event_type=event.event_type,
                payload_version=event.payload_version,
                payload=dict(event.payload),
                attempts=event.attempts + 1,
                created_at=event.created_at,
            )

        try:
            self._consumer.consume(message)
        except Exception as exc:  # noqa: BLE001 - 任何消费失败都进入重试/隔离
            logger.warning("事件 %s 第 %d 次派发失败: %s", message.event_id, message.attempts, exc)
            return self._record_failure(event_pk, message.attempts, exc)
        return self._record_success(event_pk, message.attempts)

    def _record_success(self, event_pk: int, attempt_number: int) -> str:
        now = self._now()
        with self._session_factory() as db:
            event = db.get(OutboxEvent, event_pk)
            if event is None:
                return "skipped"
            db.add(OutboxAttempt(
                event_id=event.event_id,
                attempt_number=attempt_number,
                outcome=OutboxAttempt.OUTCOME_SUCCESS,
                error=None,
                attempted_at=now,
            ))
            event.attempts = attempt_number
            event.status = OutboxEvent.STATUS_DELIVERED
            event.delivered_at = now
            event.last_error = None
            db.commit()
            return "delivered"

    def _record_failure(self, event_pk: int, attempt_number: int, error: Exception) -> str:
        now = self._now()
        error_text = str(error)[:500]
        with self._session_factory() as db:
            event = db.get(OutboxEvent, event_pk)
            if event is None:
                return "skipped"
            db.add(OutboxAttempt(
                event_id=event.event_id,
                attempt_number=attempt_number,
                outcome=OutboxAttempt.OUTCOME_FAILURE,
                error=error_text,
                attempted_at=now,
            ))
            event.attempts = attempt_number
            event.last_error = error_text
            event.claimed_by = None
            event.claimed_at = None
            if attempt_number >= event.max_attempts:
                event.status = OutboxEvent.STATUS_QUARANTINED
                outcome = "quarantined"
            else:
                # 按注入时钟计算下次可领取时间
                event.status = OutboxEvent.STATUS_PENDING
                event.available_at = now + self._retry_policy(attempt_number)
                outcome = "retried"
            db.commit()
            return outcome
