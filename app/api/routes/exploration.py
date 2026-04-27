"""
Exploration API Routes
Endpoints for data exploration and quality analysis
"""
from fastapi import APIRouter, status, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional
from pydantic import BaseModel, Field

from app.db.session import get_db
from app.models.dataset import Dataset, DatasetStatus
from app.models.job import Job, JobStatus, JobStage
from app.services.exploration_service import ExplorationService
from app.services.dataset_service import DatasetService
from app.core.config import settings


router = APIRouter(prefix="/exploration", tags=["exploration"])


class ExplorationStartRequest(BaseModel):
    """Request model for starting exploration"""
    dataset_id: int = Field(..., description="ID of the dataset to explore")
    run_async: bool = Field(default=False, description="Run exploration in background")


class ExplorationResponse(BaseModel):
    """Response model for exploration endpoints"""
    success: bool
    message: str
    dataset_id: int
    job_id: Optional[int] = None
    summary_path: Optional[str] = None


@router.post("/start", status_code=status.HTTP_202_ACCEPTED, response_model=ExplorationResponse)
async def start_exploration(
    request: ExplorationStartRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Start data exploration for a dataset.
    
    - **dataset_id**: ID of the dataset to explore
    - **run_async**: If true, run exploration in background
    - Returns exploration status and summary path
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, request.dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {request.dataset_id} not found"
        )
    
    if not dataset.file_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Dataset file path not found"
        )
    
    # Create job for tracking
    job = Job(
        name=f"Exploration for dataset {request.dataset_id}",
        stage=JobStage.PREPROCESSING,
        status=JobStatus.PENDING,
        dataset_id=request.dataset_id
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    if request.run_async:
        # Run in background
        background_tasks.add_task(
            ExplorationService.trigger_exploration,
            db=db,
            dataset_id=request.dataset_id,
            job_id=job.id
        )
        
        return ExplorationResponse(
            success=True,
            message="Exploration started in background",
            dataset_id=request.dataset_id,
            job_id=job.id
        )
    else:
        # Run synchronously
        success, message, summary = ExplorationService.trigger_exploration(
            db=db,
            dataset_id=request.dataset_id,
            job_id=job.id
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=message
            )
        
        # Save summary metrics
        if summary:
            summary_path = ExplorationService.save_summary_metrics(
                db=db,
                dataset_id=request.dataset_id,
                summary=summary
            )
        else:
            summary_path = None
        
        return ExplorationResponse(
            success=True,
            message=message,
            dataset_id=request.dataset_id,
            job_id=job.id,
            summary_path=summary_path
        )


@router.get("/summary/{dataset_id}", status_code=status.HTTP_200_OK)
async def get_exploration_summary(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get exploration summary for a dataset.
    
    - **dataset_id**: ID of the dataset
    - Returns exploration summary with quality metrics
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )
    
    # Get summary
    summary = ExplorationService.get_exploration_summary(db, dataset_id)
    
    if not summary:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exploration summary not found. Run exploration first."
        )
    
    return {
        "dataset_id": dataset_id,
        "summary": summary
    }


@router.get("/status/{job_id}", status_code=status.HTTP_200_OK)
async def get_exploration_status(
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get exploration job status.
    
    - **job_id**: ID of the exploration job
    - Returns job status and progress
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found"
        )
    
    return {
        "job_id": job.id,
        "status": job.status.value,
        "stage": job.stage.value,
        "progress_percentage": job.progress_percentage,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "error_message": job.error_message,
        "logs": job.logs
    }


@router.get("/file/{dataset_id}", status_code=status.HTTP_200_OK)
async def get_exploration_file(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get path to exploration summary file.
    
    - **dataset_id**: ID of the dataset
    - Returns file path for the exploration summary
    """
    # Validate dataset exists
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )
    
    # Get summary file path
    file_path = ExplorationService.get_exploration_summary_file(db, dataset_id)
    
    if not file_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exploration summary file not found"
        )
    
    return {
        "dataset_id": dataset_id,
        "file_path": file_path,
        "file_name": file_path.split("/")[-1] if "/" in file_path else file_path.split("\\")[-1]
    }