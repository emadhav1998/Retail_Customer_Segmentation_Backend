"""
Job schemas for request/response validation
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from app.models.job import JobStatus, JobStage


class JobBase(BaseModel):
    """Base job schema"""
    name: str = Field(..., description="Job name")


class JobCreate(JobBase):
    """Schema for creating a new job"""
    dataset_id: Optional[int] = Field(None, description="Associated dataset ID")
    stage: JobStage = Field(default=JobStage.UPLOAD, description="Current pipeline stage")
    status: JobStatus = Field(default=JobStatus.PENDING, description="Job status")


class JobUpdate(BaseModel):
    """Schema for updating job"""
    status: Optional[JobStatus] = None
    stage: Optional[JobStage] = None
    progress_percentage: Optional[int] = Field(None, ge=0, le=100)
    error_message: Optional[str] = None
    logs: Optional[str] = None
    completed_at: Optional[datetime] = None


class JobResponse(JobBase):
    """Schema for job response"""
    id: int
    stage: JobStage
    status: JobStatus
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    dataset_id: Optional[int]
    error_message: Optional[str]
    progress_percentage: int
    logs: Optional[str]
    
    class Config:
        from_attributes = True


class JobListResponse(BaseModel):
    """Schema for listing jobs"""
    jobs: list[JobResponse]
    total: int