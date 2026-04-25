"""
Segment schemas for request/response validation
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from app.models.segment import SegmentName


class SegmentBase(BaseModel):
    """Base segment schema"""
    customer_id: str = Field(..., description="Customer identifier")
    segment_name: SegmentName = Field(..., description="Segment name")


class SegmentCreate(SegmentBase):
    """Schema for creating segment results"""
    rfm_score: int = Field(..., ge=1, le=5, description="Overall RFM score")
    recency_score: int = Field(..., ge=1, le=5, description="Recency score")
    frequency_score: int = Field(..., ge=1, le=5, description="Frequency score")
    monetary_score: int = Field(..., ge=1, le=5, description="Monetary score")
    recency_value: Optional[float] = Field(None, description="Recency value in days")
    frequency_value: Optional[int] = Field(None, description="Frequency value (number of purchases)")
    monetary_value: Optional[float] = Field(None, description="Monetary value in currency")
    cluster_id: Optional[int] = Field(None, description="Cluster ID from K-means")
    job_id: Optional[int] = Field(None, description="Associated job ID")


class SegmentUpdate(BaseModel):
    """Schema for updating segment"""
    segment_name: Optional[SegmentName] = None
    rfm_score: Optional[int] = Field(None, ge=1, le=5)
    cluster_id: Optional[int] = None


class SegmentResponse(SegmentBase):
    """Schema for segment response"""
    id: int
    rfm_score: int
    recency_score: int
    frequency_score: int
    monetary_score: int
    recency_value: Optional[float]
    frequency_value: Optional[int]
    monetary_value: Optional[float]
    cluster_id: Optional[int]
    job_id: Optional[int]
    created_at: datetime
    
    class Config:
        from_attributes = True


class SegmentListResponse(BaseModel):
    """Schema for listing segments"""
    segments: list[SegmentResponse]
    total: int


class SegmentSummary(BaseModel):
    """Schema for segment summary statistics"""
    segment_name: SegmentName
    customer_count: int
    avg_rfm_score: float
    avg_monetary_value: float
    percentage: float