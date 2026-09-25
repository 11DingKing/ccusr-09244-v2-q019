"""发件箱查询与手动派发接口。

隔离状态（quarantined）的事件可通过 ``GET /outbox/events?status=quarantined`` 查询。
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.database import SessionLocal, get_db
from app.models import OutboxAttempt, OutboxEvent
from app.schemas.outbox import (
    DispatchReportResponse,
    OutboxAttemptResponse,
    OutboxEventResponse,
)
from app.services.analytics_consumer import AnalyticsConsumer
from app.services.dispatcher import OutboxDispatcher

router = APIRouter()


def build_dispatcher(request: Request) -> OutboxDispatcher:
    """按应用状态构造派发器，测试可替换 session_factory / consumer。"""
    factory = getattr(request.app.state, "outbox_session_factory", None) or SessionLocal
    consumer = getattr(request.app.state, "outbox_consumer", None)
    if consumer is None:
        consumer = AnalyticsConsumer(factory)
    return OutboxDispatcher(factory, consumer)


@router.get("/outbox/events", response_model=List[OutboxEventResponse], tags=["发件箱"])
def list_outbox_events(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    status: Optional[str] = Query(None, description="状态过滤：pending/claimed/delivered/quarantined"),
    event_type: Optional[str] = Query(None, description="事件类型过滤"),
    db: Session = Depends(get_db),
):
    query = db.query(OutboxEvent)
    if status:
        query = query.filter(OutboxEvent.status == status)
    if event_type:
        query = query.filter(OutboxEvent.event_type == event_type)
    return query.order_by(OutboxEvent.id).offset(skip).limit(limit).all()


@router.get("/outbox/events/{event_id}", response_model=OutboxEventResponse, tags=["发件箱"])
def get_outbox_event(event_id: str, db: Session = Depends(get_db)):
    event = db.query(OutboxEvent).filter(OutboxEvent.event_id == event_id).first()
    if not event:
        raise HTTPException(status_code=404, detail="事件不存在")
    return event


@router.get("/outbox/events/{event_id}/attempts", response_model=List[OutboxAttemptResponse], tags=["发件箱"])
def list_outbox_attempts(event_id: str, db: Session = Depends(get_db)):
    event = db.query(OutboxEvent).filter(OutboxEvent.event_id == event_id).first()
    if not event:
        raise HTTPException(status_code=404, detail="事件不存在")
    return (
        db.query(OutboxAttempt)
        .filter(OutboxAttempt.event_id == event_id)
        .order_by(OutboxAttempt.attempt_number)
        .all()
    )


@router.post("/outbox/dispatch", response_model=DispatchReportResponse, tags=["发件箱"])
def dispatch_outbox_once(request: Request):
    """手动触发一轮派发（后台派发线程之外的补充入口）。"""
    dispatcher = build_dispatcher(request)
    report = dispatcher.dispatch_pending()
    return DispatchReportResponse(**report.as_dict())
