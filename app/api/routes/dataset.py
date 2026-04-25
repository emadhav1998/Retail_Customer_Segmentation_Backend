"""Dataset management routes"""
from fastapi import APIRouter, status, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.db.session import get_db
from app.models.dataset import DatasetStatus
from app.schemas.dataset import (
    DatasetResponse,
    DatasetListResponse,
    DatasetUploadResponse,
    DatasetDetailResponse,
    DatasetValidationResult
)
from app.schemas.common import ErrorResponse, PaginatedResponse
from app.services.dataset_service import DatasetService
from app.services.file_service import FileService
from app.core.config import settings


router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.post("/upload", status_code=status.HTTP_201_CREATED, response_model=DatasetUploadResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Upload and process a new dataset.
    
    - **file**: CSV or Excel file containing retail transaction data
    - Returns dataset ID and validation status
    """
    # Step 1: Validate file extension
    is_valid, error_msg = FileService.validate_upload(file)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_msg)
    
    # Step 2: Read file content
    try:
        file_content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error reading file: {str(e)}")
    
    # Step 3: Save raw dataset and create initial record
    try:
        dataset, file_path = DatasetService.save_raw_dataset(
            db=db,
            file_content=file_content,
            original_filename=file.filename
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error saving file: {str(e)}")
    
    # Step 4: Validate columns and update dataset status
    try:
        columns, row_count = DatasetService.read_headers(file_path)
        is_valid, missing, extra = DatasetService.validate_required_columns(columns)
        
        if not is_valid:
            dataset.status = DatasetStatus.FAILED
            dataset.error_message = f"Missing required columns: {', '.join(missing)}"
            dataset.row_count = row_count
            db.commit()
            
            return DatasetUploadResponse(
                id=dataset.id,
                file_name=dataset.file_name,
                original_file_name=dataset.original_file_name,
                row_count=dataset.row_count,
                status=dataset.status,
                message=f"Validation failed. Missing columns: {', '.join(missing)}"
            )
        
        # Update dataset with row count
        dataset.row_count = row_count
        dataset.status = DatasetStatus.PENDING
        db.commit()
        db.refresh(dataset)
        
        return DatasetUploadResponse(
            id=dataset.id,
            file_name=dataset.file_name,
            original_file_name=dataset.original_file_name,
            row_count=dataset.row_count,
            status=dataset.status,
            message="Dataset uploaded and validated successfully"
        )
        
    except Exception as e:
        dataset.status = DatasetStatus.FAILED
        dataset.error_message = str(e)
        db.commit()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error processing dataset: {str(e)}")


@router.get("/", response_model=PaginatedResponse[DatasetResponse])
async def list_datasets(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status_filter: Optional[DatasetStatus] = Query(None, description="Filter by status"),
    db: Session = Depends(get_db)
):
    """
    List all available datasets with pagination.
    
    - **page**: Page number (starting from 1)
    - **page_size**: Number of items per page (max 100)
    - **status_filter**: Filter by dataset status (optional)
    """
    skip = (page - 1) * page_size
    
    datasets, total = DatasetService.get_all_datasets(
        db=db,
        skip=skip,
        limit=page_size,
        status=status_filter
    )
    
    total_pages = (total + page_size - 1) // page_size
    
    return PaginatedResponse(
        data=datasets,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


@router.get("/{dataset_id}", response_model=DatasetDetailResponse)
async def get_dataset(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get specific dataset details by ID.
    
    - **dataset_id**: Unique dataset identifier
    - Returns detailed information including file size and columns
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    
    # Get additional details
    columns = DatasetService.get_dataset_columns(db, dataset_id)
    file_size = FileService.get_file_size(dataset.file_path) if dataset.file_path else None
    
    return DatasetDetailResponse(
        id=dataset.id,
        file_name=dataset.file_name,
        original_file_name=dataset.original_file_name,
        upload_date=dataset.upload_date,
        source_type=dataset.source_type,
        status=dataset.status,
        row_count=dataset.row_count,
        file_path=dataset.file_path,
        error_message=dataset.error_message,
        processed_at=dataset.processed_at,
        file_size=file_size,
        columns=columns
    )


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a dataset and its associated file.
    
    - **dataset_id**: Unique dataset identifier
    - Returns 204 No Content on success
    """
    success = DatasetService.delete_dataset(db, dataset_id)
    
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    
    return None


@router.post("/validate/{dataset_id}", response_model=DatasetValidationResult)
async def validate_dataset(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Validate dataset columns against required schema.
    
    - **dataset_id**: Unique dataset identifier
    - Returns validation result with missing/extra columns
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    
    columns = DatasetService.get_dataset_columns(db, dataset_id)
    
    if not columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Could not read dataset columns")
    
    is_valid, missing, extra = DatasetService.validate_required_columns(columns)
    
    return DatasetValidationResult(
        is_valid=is_valid,
        errors=missing,
        warnings=extra if extra else [],
        column_mapping=dict(zip(columns, columns))
    )
