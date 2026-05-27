from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID
from pydantic import BaseModel, Field


class JobResponse(BaseModel):
    id: UUID
    job_type: str
    status: JobStatus
    priority: int
    current_attempts: int
    max_attempts: int
    progress: int
    result: Optional[Dict[str, Any]] = None
    error_info: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    

class JobStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class JobType(str, Enum):
    EMAIL = "email"
    WEBHOOK = "webhook"
    REPORT = "report"
    BATCH = "batch"


class JobCreate(BaseModel):
    job_type: JobType = Field(..., description="job type")
    payload: Dict[str, Any] = Field(..., description="job data")
    priority: int = Field(default=0, ge=0, description="priority")
    scheduled_at: Optional[datetime] = Field(default=None, description="when to execute")


    class Config:
        from_attributes = True 
