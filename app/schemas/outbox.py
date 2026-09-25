from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class OutboxEventResponse(BaseModel):
    id: int
    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload_version: str
    payload: Dict[str, Any]
    status: str
    attempt_count: int
    max_attempts: int
    available_at: datetime
    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None
    last_error: Optional[str] = None
    dead_letter_reason: Optional[str] = None
    created_at: datetime
    delivered_at: Optional[datetime] = None
    dead_letter_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class OutboxAttemptResponse(BaseModel):
    id: int
    event_id: int
    attempt_number: int
    status: str
    consumer: Optional[str] = None
    error: Optional[str] = None
    next_available_at: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True


class OutboxEventDetailResponse(OutboxEventResponse):
    attempts: List[OutboxAttemptResponse] = Field(default_factory=list)


class OutboxReplayRequest(BaseModel):
    reset_attempts: bool = Field(False, description="是否清零已消耗的重试次数")
