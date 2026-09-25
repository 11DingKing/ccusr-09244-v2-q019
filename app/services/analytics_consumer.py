"""内部分析组件的事件消费者。

消费副作用（投影计数）与确认记录（consumed_events）在同一事务提交，
(event_id, consumer) 唯一约束保证：即使确认后进程崩溃、事件被重复派发，
重复确认也不会让副作用生效第二次。
"""

from __future__ import annotations

import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.models import AnalyticsProjection, ConsumedEvent
from app.services.dispatcher import EventConsumer, EventMessage
from app.services.outbox import Clock, as_utc_naive, system_clock

logger = logging.getLogger(__name__)


class AnalyticsConsumer(EventConsumer):
    """把业务事件累积到分析投影表的进程内消费者。"""

    def __init__(
        self,
        session_factory: sessionmaker,
        *,
        name: str = "analytics",
        clock: Clock = system_clock,
    ) -> None:
        self._session_factory = session_factory
        self._name = name
        self._clock = clock

    @property
    def name(self) -> str:
        return self._name

    def consume(self, message: EventMessage) -> None:
        now = as_utc_naive(self._clock())
        with self._session_factory() as db:
            already = (
                db.query(ConsumedEvent)
                .filter(
                    ConsumedEvent.event_id == message.event_id,
                    ConsumedEvent.consumer == self._name,
                )
                .first()
            )
            if already is not None:
                # 已确认过：跳过副作用，避免重复确认造成二次影响
                logger.info("事件 %s 已被 %s 消费，跳过", message.event_id, self._name)
                return

            metric_name = f"{message.event_type}.count"
            projection = (
                db.query(AnalyticsProjection)
                .filter(AnalyticsProjection.metric == metric_name)
                .first()
            )
            if projection is None:
                projection = AnalyticsProjection(metric=metric_name, value=0, updated_at=now)
                db.add(projection)
            projection.value += 1
            projection.updated_at = now

            db.add(ConsumedEvent(event_id=message.event_id, consumer=self._name, consumed_at=now))
            try:
                db.commit()
            except IntegrityError:
                # 并发重复确认：唯一约束拦截，视为已消费
                db.rollback()
                logger.info("事件 %s 确认冲突，按已消费处理", message.event_id)
