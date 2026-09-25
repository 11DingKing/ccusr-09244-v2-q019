"""内部分析组件：消费发件箱事件的幂等处理器。

投递语义为“至少一次”：进程在副作用完成后、确认落库前崩溃，
事件会被重新领取并重投。因此消费侧必须幂等——这里用
``inbox_event_confirmations(event_id, consumer)`` 唯一约束
作为去重闸门，确认与投影写入在同一个事务中提交：

- 首次确认：写入确认行 + 分析投影行；
- 重复确认（含 ack 丢失后的重投）：唯一约束冲突，事务回滚，
  不产生任何二次影响；处理器视为成功（幂等空操作）。
"""

from __future__ import annotations

import logging
from typing import Callable

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import AnalysisEventLog, InboxEventConfirmation
from app.events.dispatcher import DispatchedEvent

logger = logging.getLogger(__name__)


def apply_consumption(db: Session, event: DispatchedEvent, consumer: str) -> bool:
    """在给定会话内幂等落地一次消费。

    返回 True 表示本次首次生效，False 表示重复确认被忽略。
    """
    try:
        db.add(
            InboxEventConfirmation(event_id=event.id, consumer=consumer)
        )
        db.flush()  # 尽早触发唯一约束
    except IntegrityError:
        db.rollback()
        return False

    db.add(
        AnalysisEventLog(
            event_id=event.id,
            event_type=event.event_type,
            payload_version=event.payload_version,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            payload=event.payload,
            consumer=consumer,
        )
    )
    db.commit()
    return True


class AnalysisProjectionHandler:
    """派发器回调：把业务事件写入分析投影。"""

    def __init__(self, session_factory: Callable[[], Session], consumer: str = "analysis-component"):
        self._session_factory = session_factory
        self._consumer = consumer

    @property
    def consumer(self) -> str:
        return self._consumer

    def handle(self, event: DispatchedEvent) -> None:
        db = self._session_factory()
        try:
            first_time = apply_consumption(db, event, self._consumer)
            if not first_time:
                logger.info(
                    "事件 %s 已被 %s 确认过，忽略重复投递", event.id, self._consumer
                )
        finally:
            db.close()
