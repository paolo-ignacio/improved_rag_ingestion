# Pydantic schemas for request/response validation


from pydantic import BaseModel, UUID4
from datetime import datetime
from typing import Optional


class IngestionJobResponse(BaseModel):
    job_id: UUID4
    file_name: str
    status : str = "queued"
    created_at: datetime


class JobStatusResponse(BaseModel):
    job_id: UUID4
    file_name: str
    status: str
    updated_at: datetime
    detail: Optional[str] = None