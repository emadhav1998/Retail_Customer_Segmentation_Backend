"""
Cleaning Schemas
Pydantic models for cleaning API requests and responses
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class CleaningStep(str, Enum):
    """Cleaning step enumeration"""
    STEP1 = "step1"
    STEP2 = "step2"


class CleaningStatus(str, Enum):
    """Cleaning status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class CleaningStartRequest(BaseModel):
    """Request model for starting cleaning"""
    dataset_id: int = Field(..., description="ID of the dataset to clean")
    step: CleaningStep = Field(default=CleaningStep.STEP1, description="Cleaning step to run")


class CleaningResponse(BaseModel):
    """Response model for cleaning endpoints"""
    success: bool
    message: str
    job_id: Optional[int] = Field(None, description="ID of the cleaning job")


class CleaningMetadataResponse(BaseModel):
    """Response model for cleaning metadata"""
    dataset_id: int
    step: str
    input_file: str
    output_file: str
    initial_rows: int
    final_rows: int
    total_rows_removed: int
    removal_percentage: float
    duplicate_stats: Dict[str, Any]
    customerid_stats: Dict[str, Any]
    columns: List[str]
    column_count: int
    timestamp: str


class CleaningStatusResponse(BaseModel):
    """Response model for cleaning job status"""
    job_id: int
    name: str
    status: str
    progress_percentage: int
    logs: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None


class CleaningSummaryResponse(BaseModel):
    """Response model for cleaning summary"""
    success: bool
    message: str
    data: Optional[CleaningMetadataResponse] = None


# Re-export for convenience
__all__ = [
    "CleaningStep",
    "CleaningStatus",
    "CleaningStartRequest",
    "CleaningResponse",
    "CleaningMetadataResponse",
    "CleaningStatusResponse",
    "CleaningSummaryResponse"
]