"""
AI Summary API Routes
Endpoints for generating and fetching AI executive summaries
"""
from fastapi import APIRouter, status, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from app.db.session import get_db
from app.services.ai_summary_service import AISummaryService
from app.services.dataset_service import DatasetService
from app.schemas.ai_summary import AISummaryRequest, AISummaryResponse


router = APIRouter(prefix="/ai-summary", tags=["ai-summary"])


@router.post(
    "/generate",
    summary="Generate AI executive summary",
    description="Produce a structured executive summary from customer segment metrics",
    response_model=AISummaryResponse
)
async def generate_ai_summary(
    request: AISummaryRequest,
    db: Session = Depends(get_db)
):
    """
    Generate an AI executive summary from customer segments.

    - **dataset_id**: ID of the dataset
    - **segment_metrics**: Optional pre-computed metrics (computed from CSV if omitted)
    - **include_recommendations**: Include prioritised recommendations
    - **include_risk_analysis**: Include at-risk customer analysis
    - **include_growth_signals**: Include growth opportunity signals
    """
    dataset = DatasetService.get_dataset_by_id(db, request.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {request.dataset_id} not found"
        )

    # Convert schema metrics to plain dicts if provided
    external_metrics = None
    if request.segment_metrics:
        external_metrics = [m.model_dump() for m in request.segment_metrics]

    result = AISummaryService.generate_summary(
        db,
        request.dataset_id,
        include_recommendations=request.include_recommendations,
        include_risk_analysis=request.include_risk_analysis,
        include_growth_signals=request.include_growth_signals,
        external_metrics=external_metrics
    )

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': result['message'],
        'data': result['data'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/latest/{dataset_id}",
    summary="Fetch latest AI summary",
    description="Retrieve the most recently generated AI executive summary",
    response_model=AISummaryResponse
)
async def get_latest_summary(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Fetch the latest persisted AI summary for a dataset.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    summary = AISummaryService.get_latest_summary(dataset_id)

    if not summary:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No AI summary found. Please call POST /generate first."
        )

    return {
        'success': True,
        'message': 'Latest AI summary retrieved successfully',
        'data': summary,
        'timestamp': datetime.utcnow().isoformat()
    }
