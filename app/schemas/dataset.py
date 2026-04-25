"""
Dataset schemas for request/response validation
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from app.models.dataset import DatasetStatus, DatasetSourceType


class DatasetBase(BaseModel):
    """Base dataset schema"""
    file_name: str = Field(..., description="Generated file name")
    original_file_name: Optional[str] = Field(None, description="Original uploaded file name")
    source_type: DatasetSourceType = Field(default=DatasetSourceType.UPLOAD, description="Source type")


class DatasetCreate(DatasetBase):
    """Schema for creating a new dataset"""
    row_count: int = Field(default=0, description="Number of rows in the dataset")
    status: DatasetStatus = Field(default=DatasetStatus.PENDING, description="Processing status")


class DatasetUpdate(BaseModel):
    """Schema for updating dataset"""
    status: Optional[DatasetStatus] = None
    row_count: Optional[int] = None
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None


class DatasetResponse(DatasetBase):
    """Schema for dataset response"""
    id: int
    upload_date: datetime
    status: DatasetStatus
    row_count: int
    file_path: Optional[str]
    error_message: Optional[str]
    processed_at: Optional[datetime]
    
    class Config:
        from_attributes = True


class DatasetListResponse(BaseModel):
    """Schema for listing datasets"""
    datasets: list[DatasetResponse]
    total: int