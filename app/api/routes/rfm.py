"""
RFM API Routes
Endpoints for RFM (Recency, Frequency, Monetary) analysis
"""
from fastapi import APIRouter, status, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

from app.db.session import get_db
from app.models.dataset import Dataset
from app.models.job import Job, JobStatus, JobStage
from app.services.rfm_service import RFMService
from app.services.dataset_service import DatasetService
from app.core.config import settings


router = APIRouter(prefix="/rfm", tags=["rfm"])


class RecencyCalculationRequest(BaseModel):
    """Request model for recency calculation"""
    dataset_id: int = Field(..., description="ID of the dataset")
    reference_date: Optional[str] = Field(None, description="Reference date (YYYY-MM-DD). If not provided, uses max date + 1 day")


class RecencyCalculationResponse(BaseModel):
    """Response model for recency calculation"""
    success: bool
    message: str
    job_id: Optional[int] = Field(None, description="ID of the RFM job")


class RecencyStatsResponse(BaseModel):
    """Response model for recency statistics"""
    min_days: int
    max_days: int
    mean_days: float
    median_days: int
    std_dev: float


class RecencyQuartilesResponse(BaseModel):
    """Response model for recency quartiles"""
    q1_75th_percentile: float
    q2_50th_percentile: float
    q3_25th_percentile: float
    q4_max: float


class RecencyCustomerDistributionResponse(BaseModel):
    """Response model for customer distribution by recency"""
    recent_0_30_days: int
    recent_31_60_days: int
    recent_61_90_days: int
    recent_90_plus_days: int


class RecencyPreviewResponse(BaseModel):
    """Response model for recency preview"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class RecencyJobStatusResponse(BaseModel):
    """Response model for recency job status"""
    job_id: int
    name: str
    stage: str
    status: str
    progress_percentage: int
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    dataset_id: int


@router.post(
    "/calculate-recency",
    response_model=RecencyCalculationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Calculate recency metrics",
    description="Trigger recency calculation for a dataset"
)
async def calculate_recency(
    request: RecencyCalculationRequest,
    db: Session = Depends(get_db)
):
    """
    Calculate recency metrics for customer segmentation.
    
    - **dataset_id**: ID of the cleaned dataset
    - **reference_date**: Optional reference date (default: max date + 1 day)
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, request.dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {request.dataset_id} not found"
        )
    
    try:
        # Create a job for tracking
        job = Job(
            name="Recency Calculation",
            stage=JobStage.RFM_CALCULATION,
            status=JobStatus.PENDING,
            dataset_id=dataset.id,
            started_at=datetime.now()
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run recency calculation
        success, message, summary = RFMService.calculate_recency(
            db=db,
            dataset_id=dataset.id,
            job_id=job.id,
            reference_date=request.reference_date
        )
        
        if success:
            return RecencyCalculationResponse(
                success=True,
                message=message,
                job_id=job.id
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=message
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating recency: {str(e)}"
        )


@router.get(
    "/recency-preview/{dataset_id}",
    response_model=RecencyPreviewResponse,
    summary="Get recency preview",
    description="Fetch recency statistics and preview for a dataset"
)
async def get_recency_preview(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get recency calculation preview and statistics.
    
    - **dataset_id**: ID of the dataset
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )
    
    # Get recency metadata
    metadata = RFMService.get_recency_preview(db, dataset_id)
    
    if not metadata:
        return RecencyPreviewResponse(
            success=False,
            message="No recency data found. Please run recency calculation first.",
            data=None
        )
    
    # Extract summary data
    summary = metadata.get('recency_summary', {})
    
    return RecencyPreviewResponse(
        success=True,
        message="Recency preview retrieved successfully",
        data={
            'reference_date': metadata.get('reference_date_stats', {}).get('reference_date'),
            'unique_customers': summary.get('unique_customers'),
            'total_transactions': summary.get('total_transactions'),
            'recency_stats': summary.get('recency_stats'),
            'recency_quartiles': summary.get('recency_quartiles'),
            'customer_distribution': summary.get('customer_distribution')
        }
    )


@router.get(
    "/recency-status/{job_id}",
    response_model=RecencyJobStatusResponse,
    summary="Get recency job status",
    description="Fetch the status of a recency calculation job"
)
async def get_recency_status(
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get recency calculation job status.
    
    - **job_id**: ID of the recency job
    """
    status_data = RFMService.get_rfm_job_status(db, job_id)
    
    if not status_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"RFM job with ID {job_id} not found"
        )
    
    return RecencyJobStatusResponse(**status_data)


@router.get(
    "/recency-data/{dataset_id}",
    summary="Get customer recency data",
    description="Fetch customer-level recency data preview"
)
async def get_customer_recency_data(
    dataset_id: int,
    limit: Optional[int] = Query(10, ge=1, le=1000, description="Number of records to return"),
    db: Session = Depends(get_db)
):
    """
    Get customer-level recency data preview.
    
    - **dataset_id**: ID of the dataset
    - **limit**: Number of records to return (default: 10, max: 1000)
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )
    
    # Get recency data
    data = RFMService.get_customer_recency_data(db, dataset_id, limit=limit)
    
    if not data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No recency data found. Please run recency calculation first."
        )
    
    return {
        'success': True,
        'message': f'Retrieved {len(data)} customer recency records',
        'count': len(data),
        'data': data
    }