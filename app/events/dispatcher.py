"""进程内发件箱派发器。

职责：
- 按 id 顺序（FIFO）领取到期事件，领取动作是带状态守卫的原子
  UPDATE，多个派发器/线程并发领取不会拿到同一行；
- 调用注册的处理器完成消费侧副作用，成功与失败都写入尝试留痕；
- 失败按可注入时钟计算指数退避重试时间，超过 ``max_attempts``
  转入 ``dead_letter`` 隔离状态；
- 进程崩溃会留下 ``processing`` 僵死行，锁超时后由下一轮回收，
  配合消费侧幂等实现“至少一次投递 + 恰好一次效果”。
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Protocol

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session

from app.models import OutboxEvent, OutboxDeliveryAttempt

logger = logging.getLogger(__name__)

STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DELIVERED = "delivered"
STATUS_DEAD_LETTER = "dead_letter"

ATTEMPT_SUCCEEDED = "succeeded"
ATTEMPT_FAILED = "failed"
ATTEMPT_RECLAIMED = "reclaimed"


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FixedClock:
    """测试用时钟：可显式推进，避免依赖真实 sleep。"""

    def __init__(self, start: Optional[datetime] = None):
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        self._now = self._now + delta
        return self._now


def compute_next_retry_at(
    now: datetime,
    failed_attempt_number: int,
    base_delay: float,
    backoff_factor: float,
    max_delay: float,
) -> datetime:
    """第 ``failed_attempt_number`` 次失败后的下次可领取时间。

    退避序列为 base, base*factor, base*factor**2 ...，并被 max_delay 截断。
    """
    delay = min(base_delay * (backoff_factor ** (failed_attempt_number - 1)), max_delay)
    return now + timedelta(seconds=delay)


@dataclass(frozen=True)
class DispatchedEvent:
    id: int
    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload_version: str
    payload: dict
    attempt_number: int


class EventHandler(Protocol):
    def handle(self, event: DispatchedEvent) -> None:
        """处理器抛出异常即视为本次投递失败；不得自行提交投递结果。"""


@dataclass
class DispatchSummary:
    claimed: int = 0
    succeeded: int = 0
    failed: int = 0
    retried: int = 0
    dead_lettered: int = 0
    reclaimed: int = 0

    @property
    def processed(self) -> int:
        return self.succeeded + self.failed


class EventDispatcher:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        handlers: Dict[str, EventHandler],
        *,
        clock: Optional[Clock] = None,
        consumer_name: str = "in-process",
        batch_size: int = 10,
        lock_timeout: timedelta = timedelta(minutes=5),
        base_retry_delay: float = 5.0,
        backoff_factor: float = 2.0,
        max_retry_delay: float = 3600.0,
    ):
        self._session_factory = session_factory
        self._handlers = dict(handlers)
        self._clock = clock or SystemClock()
        self._consumer_name = consumer_name
        self._batch_size = batch_size
        self._lock_timeout = lock_timeout
        self._base_retry_delay = base_retry_delay
        self._backoff_factor = backoff_factor
        self._max_retry_delay = max_retry_delay
        self._owner_seq = itertools.count(1)
        self._owner = f"{consumer_name}-{id(self)}-{next(self._owner_seq)}"

    # ---------- 领取 ----------

    def _due_predicate(self, now: datetime):
        stale_cutoff = now - self._lock_timeout
        return or_(
            and_(
                OutboxEvent.status == STATUS_PENDING,
                OutboxEvent.available_at <= now,
            ),
            and_(
                OutboxEvent.status == STATUS_PROCESSING,
                OutboxEvent.locked_at < stale_cutoff,
            ),
        )

    def claim_due(self, limit: Optional[int] = None) -> List[DispatchedEvent]:
        """原子领取一批到期事件（含锁超时的僵死事件），按 id 升序。"""
        limit = limit or self._batch_size
        db = self._session_factory()
        try:
            now = self._clock.now()
            due = self._due_predicate(now)

            ids = list(
                db.execute(
                    select(OutboxEvent.id).where(due)
                    .order_by(OutboxEvent.id)
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            if not ids:
                return []

            # 候选中当前处于 processing 的即僵死行；最终是否被本派发器
            # 抢到以条件 UPDATE 的归属为准，避免为他人抢走的行误记回收
            stale_ids = {
                row[0]
                for row in db.execute(
                    select(OutboxEvent.id).where(
                        OutboxEvent.id.in_(ids),
                        OutboxEvent.status == STATUS_PROCESSING,
                    )
                ).all()
            }

            # 状态守卫保证并发下只有真正仍可领取的行会被更新
            db.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id.in_(ids), due)
                .values(status=STATUS_PROCESSING, locked_at=now, locked_by=self._owner)
            )
            db.commit()

            rows = (
                db.query(OutboxEvent)
                .filter(
                    OutboxEvent.id.in_(ids),
                    OutboxEvent.status == STATUS_PROCESSING,
                    OutboxEvent.locked_by == self._owner,
                )
                .order_by(OutboxEvent.id)
                .all()
            )
            if not rows:
                return []

            owned_ids = {row.id for row in rows}
            reclaimed_ids = owned_ids & stale_ids
            if reclaimed_ids:
                for event_id in reclaimed_ids:
                    db.add(
                        OutboxDeliveryAttempt(
                            event_id=event_id,
                            attempt_number=0,
                            status=ATTEMPT_RECLAIMED,
                            consumer=self._consumer_name,
                        )
                    )
                db.commit()

            return [
                DispatchedEvent(
                    id=row.id,
                    event_type=row.event_type,
                    aggregate_type=row.aggregate_type,
                    aggregate_id=row.aggregate_id,
                    payload_version=row.payload_version,
                    payload=dict(row.payload or {}),
                    attempt_number=(row.attempt_count or 0) + 1,
                )
                for row in rows
            ]
        finally:
            db.close()

    # ---------- 结果记录 ----------

    def _record_success(self, event: DispatchedEvent, now: datetime) -> None:
        db = self._session_factory()
        try:
            db.add(
                OutboxDeliveryAttempt(
                    event_id=event.id,
                    attempt_number=event.attempt_number,
                    status=ATTEMPT_SUCCEEDED,
                    consumer=self._consumer_name,
                )
            )
            db.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id == event.id)
                .values(
                    status=STATUS_DELIVERED,
                    attempt_count=OutboxEvent.attempt_count + 1,
                    delivered_at=now,
                    locked_at=None,
                    locked_by=None,
                    last_error=None,
                )
            )
            db.commit()
        finally:
            db.close()

    def _record_failure(self, event: DispatchedEvent, error: BaseException, now: datetime) -> str:
        """记录失败并返回事件的新状态（pending/dead_letter）。"""
        db = self._session_factory()
        try:
            event_row = db.query(OutboxEvent).filter(OutboxEvent.id == event.id).first()
            max_attempts = event_row.max_attempts if event_row else event.attempt_number
            exhausted = event.attempt_number >= max_attempts
            message = f"{type(error).__name__}: {error}"

            if exhausted:
                new_status = STATUS_DEAD_LETTER
                next_available_at = None
                dead_letter_reason = message
                dead_letter_at = now
            else:
                new_status = STATUS_PENDING
                next_available_at = compute_next_retry_at(
                    now,
                    event.attempt_number,
                    self._base_retry_delay,
                    self._backoff_factor,
                    self._max_retry_delay,
                )
                dead_letter_reason = None
                dead_letter_at = None

            db.add(
                OutboxDeliveryAttempt(
                    event_id=event.id,
                    attempt_number=event.attempt_number,
                    status=ATTEMPT_FAILED,
                    consumer=self._consumer_name,
                    error=message[:2000],
                    next_available_at=next_available_at,
                )
            )
            db.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id == event.id)
                .values(
                    status=new_status,
                    attempt_count=OutboxEvent.attempt_count + 1,
                    available_at=next_available_at or now,
                    locked_at=None,
                    locked_by=None,
                    last_error=message[:2000],
                    dead_letter_reason=dead_letter_reason,
                    dead_letter_at=dead_letter_at,
                )
            )
            db.commit()
            return new_status
        finally:
            db.close()

    # ---------- 派发 ----------

    def deliver(self, events: List["DispatchedEvent"]) -> DispatchSummary:
        """投递已领取的事件并记录每次尝试结果。"""
        summary = DispatchSummary(claimed=len(events))
        for event in events:
            now = self._clock.now()
            handler = self._handlers.get(event.event_type)
            try:
                if handler is None:
                    raise KeyError(f"没有注册的事件处理器: {event.event_type}")
                handler.handle(event)
                # 消费副作用已提交；若确认落库失败，按未确认排回重试，
                # 重投时由消费侧幂等保证不产生二次效果
                self._record_success(event, now)
            except Exception as exc:  # 任何失败都不能中断派发循环
                logger.warning("事件 %s 第 %s 次投递失败: %s", event.id, event.attempt_number, exc)
                try:
                    new_status = self._record_failure(event, exc, now)
                except Exception:  # 连失败结果都无法落库：保持锁定，等锁超时回收
                    logger.exception("事件 %s 失败结果落库失败，等待锁超时回收", event.id)
                    summary.failed += 1
                    continue
                summary.failed += 1
                if new_status == STATUS_DEAD_LETTER:
                    summary.dead_lettered += 1
                else:
                    summary.retried += 1
                continue

            summary.succeeded += 1
        return summary

    def process_batch(self, limit: Optional[int] = None) -> DispatchSummary:
        events = self.claim_due(limit)
        return self.deliver(events)


class BackgroundDispatcher:
    """后台轮询线程包装；失败只记录日志，等待下一轮。"""

    def __init__(self, dispatcher: EventDispatcher, poll_interval: float = 1.0):
        self._dispatcher = dispatcher
        self._poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="outbox-dispatcher", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._dispatcher.process_batch()
            except Exception:
                logger.exception("发件箱派发轮询异常")
            self._stop.wait(self._poll_interval)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
