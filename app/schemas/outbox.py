from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime


class OutboxEventResponse(BaseModel):
    id: int
    event_id: str
    event_type: str
    payload_version: int
    payload: Dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    available_at: datetime
    claimed_by: Optional[str] = None
    claimed_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    last_error: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class OutboxAttemptResponse(BaseModel):
    id: int
    event_id: str
    attempt_number: int
    outcome: str
    error: Optional[str] = None
    attempted_at: datetime

    class Config:
        from_attributes = True


class DispatchReportResponse(BaseModel):
    claimed: int
    delivered: int
    retried: int
    quarantined: int
    recovered: int
    errors: List[str]
