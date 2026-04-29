"""
Cleaning API Routes
Endpoints for data cleaning operations
"""
from fastapi import APIRouter, status, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional

from app.db.session import get_db
from app.models.dataset import Dataset, DatasetStatus
from app.models.job import Job, JobStatus, JobStage
from app.services.cleaning_service import CleaningService
from app.services.dataset_service import DatasetService
from app.schemas.cleaning import (
    CleaningStartRequest,
    CleaningResponse,
    CleaningMetadataResponse,
    CleaningStatusResponse,
    CleaningSummaryResponse
)
from app.core.config import settings


router = APIRouter(prefix="/cleaning", tags=["cleaning"])


@router.post(
    "/start",
    response_model=CleaningResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start data cleaning",
    description="Trigger cleaning step 1 for a dataset"
)
async def start_cleaning(
    request: CleaningStartRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Start data cleaning process for a dataset.
    
    - **dataset_id**: ID of the dataset to clean
    - **step**: Cleaning step to run (default: step1)
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, request.dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {request.dataset_id} not found"
        )
    
    # Check if dataset is ready for cleaning
    if dataset.status not in [DatasetStatus.UPLOADED, DatasetStatus.VALIDATED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Dataset must be in UPLOADED or VALIDATED status. Current status: {dataset.status}"
        )
    
    try:
        # Create a job for tracking
        job = Job(
            name=f"Cleaning Step {request.step.value}",
            stage=JobStage.PREPROCESSING,
            status=JobStatus.PENDING,
            dataset_id=dataset.id,
            started_at=datetime.now()
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run cleaning (synchronously for now)
        success, message, summary = CleaningService.run_cleaning_step1(
            db=db,
            dataset_id=dataset.id,
            job_id=job.id
        )
        
        if success:
            return CleaningResponse(
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
            detail=f"Error starting cleaning: {str(e)}"
        )


@router.post(
    "/start-step2",
    response_model=CleaningResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start data cleaning step 2",
    description="Trigger cleaning step 2 for a dataset (requires step 1 completion)"
)
async def start_cleaning_step2(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Start data cleaning step 2 process for a dataset.
    
    - **dataset_id**: ID of the dataset to clean
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )
    
    # Check if step 1 has been completed
    import os
    output_dir = settings.processed_data_path
    step1_file = os.path.join(output_dir, 'cleaned_step1.csv')
    
    if not os.path.exists(step1_file):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cleaning step 1 must be completed before running step 2"
        )
    
    try:
        # Create a job for tracking
        job = Job(
            name="Cleaning Step 2",
            stage=JobStage.PREPROCESSING,
            status=JobStatus.PENDING,
            dataset_id=dataset.id,
            started_at=datetime.now()
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run cleaning step 2
        success, message, summary = CleaningService.run_cleaning_step2(
            db=db,
            dataset_id=dataset.id,
            job_id=job.id
        )
        
        if success:
            return CleaningResponse(
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
            detail=f"Error starting cleaning step 2: {str(e)}"
        )


@router.get(
    "/metadata-step2/{dataset_id}",
    response_model=CleaningSummaryResponse,
    summary="Get cleaning step 2 output metadata",
    description="Fetch cleaning step 2 output metadata for a dataset"
)
async def get_cleaning_step2_metadata(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get cleaning step 2 metadata for a dataset.
    
    - **dataset_id**: ID of the dataset
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )
    
    # Get cleaning step 2 metadata
    metadata = CleaningService.get_cleaning_step2_metadata(db, dataset_id)
    
    if not metadata:
        return CleaningSummaryResponse(
            success=False,
            message="No cleaning step 2 metadata found for this dataset"
        )
    
    return CleaningSummaryResponse(
        success=True,
        message="Cleaning step 2 metadata retrieved successfully",
        data=CleaningMetadataResponse(
            dataset_id=dataset_id,
            step=metadata.get('step', 'step2'),
            input_file=metadata.get('input_file', ''),
            output_file=metadata.get('output_file', ''),
            initial_rows=metadata.get('initial_rows', 0),
            final_rows=metadata.get('final_rows', 0),
            total_rows_removed=metadata.get('total_rows_removed', 0),
            removal_percentage=metadata.get('removal_percentage', 0),
            duplicate_stats=metadata.get('quantity_stats', {}),
            customerid_stats=metadata.get('price_stats', {}),
            columns=metadata.get('columns', []),
            column_count=metadata.get('column_count', 0),
            timestamp=metadata.get('timestamp', '')
        )
    )


@router.get(
    "/metadata/{dataset_id}",
    response_model=CleaningSummaryResponse,
    summary="Get cleaning output metadata",
    description="Fetch cleaning output metadata for a dataset"
)
async def get_cleaning_metadata(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get cleaning metadata for a dataset.
    
    - **dataset_id**: ID of the dataset
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )
    
    # Get cleaning metadata
    metadata = CleaningService.get_cleaning_output_metadata(db, dataset_id)
    
    if not metadata:
        return CleaningSummaryResponse(
            success=False,
            message="No cleaning metadata found for this dataset"
        )
    
    return CleaningSummaryResponse(
        success=True,
        message="Cleaning metadata retrieved successfully",
        data=CleaningMetadataResponse(
            dataset_id=dataset_id,
            step=metadata.get('step', 'step1'),
            input_file=metadata.get('input_file', ''),
            output_file=metadata.get('output_file', ''),
            initial_rows=metadata.get('initial_rows', 0),
            final_rows=metadata.get('final_rows', 0),
            total_rows_removed=metadata.get('total_rows_removed', 0),
            removal_percentage=metadata.get('removal_percentage', 0),
            duplicate_stats=metadata.get('duplicate_stats', {}),
            customerid_stats=metadata.get('customerid_stats', {}),
            columns=metadata.get('columns', []),
            column_count=metadata.get('column_count', 0),
            timestamp=metadata.get('timestamp', '')
        )
    )


@router.get(
    "/status/{job_id}",
    response_model=CleaningStatusResponse,
    summary="Get cleaning job status",
    description="Fetch the status of a cleaning job"
)
async def get_cleaning_status(
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get cleaning job status.
    
    - **job_id**: ID of the cleaning job
    """
    status_data = CleaningService.get_cleaning_status(db, job_id)
    
    if not status_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cleaning job with ID {job_id} not found"
        )
    
    return CleaningStatusResponse(**status_data)


# Import datetime for job creation
from datetime import datetime