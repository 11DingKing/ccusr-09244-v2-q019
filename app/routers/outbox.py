"""发件箱管理接口：查询事件、尝试留痕与死信重投。"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.outbox import (
    OutboxAttemptResponse,
    OutboxEventDetailResponse,
    OutboxEventResponse,
    OutboxReplayRequest,
)
from app.services import outbox_admin

router = APIRouter()


@router.get("/outbox/events", response_model=List[OutboxEventResponse], tags=["发件箱"])
def list_outbox_events(
    status: Optional[str] = Query(None, description="状态过滤：pending/processing/delivered/dead_letter"),
    event_type: Optional[str] = Query(None, description="事件类型过滤"),
    aggregate_id: Optional[str] = Query(None, description="聚合实体ID过滤"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    if status and status not in outbox_admin.VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"非法状态，允许值：{', '.join(sorted(outbox_admin.VALID_STATUSES))}",
        )
    return outbox_admin.list_events(
        db, status=status, event_type=event_type, aggregate_id=aggregate_id, limit=limit
    )


@router.get("/outbox/events/{event_id}", response_model=OutboxEventDetailResponse, tags=["发件箱"])
def get_outbox_event(event_id: int, db: Session = Depends(get_db)):
    event = outbox_admin.get_event(db, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="发件箱事件不存在")
    return {
        "id": event.id,
        "event_type": event.event_type,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": event.aggregate_id,
        "payload_version": event.payload_version,
        "payload": event.payload,
        "status": event.status,
        "attempt_count": event.attempt_count,
        "max_attempts": event.max_attempts,
        "available_at": event.available_at,
        "locked_at": event.locked_at,
        "locked_by": event.locked_by,
        "last_error": event.last_error,
        "dead_letter_reason": event.dead_letter_reason,
        "created_at": event.created_at,
        "delivered_at": event.delivered_at,
        "dead_letter_at": event.dead_letter_at,
        "attempts": outbox_admin.list_attempts(db, event_id),
    }


@router.get("/outbox/events/{event_id}/attempts", response_model=List[OutboxAttemptResponse], tags=["发件箱"])
def list_outbox_attempts(event_id: int, db: Session = Depends(get_db)):
    event = outbox_admin.get_event(db, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="发件箱事件不存在")
    return outbox_admin.list_attempts(db, event_id)


@router.post("/outbox/events/{event_id}/replay", response_model=OutboxEventResponse, tags=["发件箱"])
def replay_outbox_event(event_id: int, req: OutboxReplayRequest, db: Session = Depends(get_db)):
    try:
        return outbox_admin.replay_dead_letter(
            db, event_id, reset_attempts=req.reset_attempts
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="发件箱事件不存在")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
