"""发件箱组件的对外装配入口。"""

from __future__ import annotations

from datetime import timedelta
from typing import Callable, Dict, Optional

from sqlalchemy.orm import Session

from app.events.contract import (
    EVENT_ANNOTATION_APPROVED,
    EVENT_DATASET_PUBLISHED,
    EVENT_DATASET_VERSION_REVOKED,
    KNOWN_EVENT_TYPES,
)
from app.events.consumer import AnalysisProjectionHandler
from app.events.dispatcher import Clock, EventDispatcher

__all__ = [
    "EVENT_ANNOTATION_APPROVED",
    "EVENT_DATASET_PUBLISHED",
    "EVENT_DATASET_VERSION_REVOKED",
    "KNOWN_EVENT_TYPES",
    "build_dispatcher",
]


def build_dispatcher(
    session_factory: Callable[[], Session],
    *,
    clock: Optional[Clock] = None,
    consumer_name: str = "analysis-component",
    batch_size: int = 10,
    base_retry_delay: float = 5.0,
    backoff_factor: float = 2.0,
    max_retry_delay: float = 3600.0,
    lock_timeout: timedelta = timedelta(minutes=5),
) -> EventDispatcher:
    """构造派发器并为全部业务事件注册分析投影处理器。"""
    handler = AnalysisProjectionHandler(session_factory, consumer=consumer_name)
    handlers: Dict[str, object] = {event_type: handler for event_type in KNOWN_EVENT_TYPES}
    return EventDispatcher(
        session_factory,
        handlers,
        clock=clock,
        consumer_name=consumer_name,
        batch_size=batch_size,
        lock_timeout=lock_timeout,
        base_retry_delay=base_retry_delay,
        backoff_factor=backoff_factor,
        max_retry_delay=max_retry_delay,
    )
