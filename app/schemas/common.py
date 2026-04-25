"""
Common schemas for API responses
"""
from pydantic import BaseModel, Field
from typing import Optional, Any, Generic, TypeVar
from datetime import datetime


T = TypeVar('T')


class ResponseBase(BaseModel):
    """Base response model"""
    success: bool = Field(default=True, description="Whether the request was successful")
    message: Optional[str] = Field(None, description="Response message")


class DataResponse(ResponseBase, Generic[T]):
    """Generic data response"""
    data: Optional[T] = Field(None, description="Response data")


class ErrorResponse(ResponseBase):
    """Error response model"""
    success: bool = Field(default=False, description="Success status")
    error_code: Optional[str] = Field(None, description="Error code for debugging")
    details: Optional[dict] = Field(None, description="Additional error details")


class PaginationParams(BaseModel):
    """Pagination parameters"""
    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=20, ge=1, le=100, description="Items per page")


class PaginatedResponse(ResponseBase, Generic[T]):
    """Paginated response"""
    data: list[T] = Field(default_factory=list, description="List of items")
    total: int = Field(default=0, description="Total number of items")
    page: int = Field(default=1, description="Current page")
    page_size: int = Field(default=20, description="Items per page")
    total_pages: int = Field(default=0, description="Total number of pages")


class TimestampMixin(BaseModel):
    """Mixin for timestamp fields"""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None


class SuccessResponse(ResponseBase):
    """Simple success response"""
    pass