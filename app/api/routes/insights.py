"""
Insights API Routes
Endpoints for generating business insights from customer segmentation
"""
from fastapi import APIRouter, status, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
import os
import pandas as pd

from app.db.session import get_db
from app.models.dataset import Dataset
from app.services.insight_service import InsightService
from app.services.dataset_service import DatasetService
from app.core.config import settings


router = APIRouter(prefix="/insights", tags=["insights"])


class InsightRequest(BaseModel):
    """Request model for insights generation"""
    dataset_id: int = Field(..., description="ID of the dataset")


class InsightResponse(BaseModel):
    """Response model for insights"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


@router.post(
    "/generate-insights",
    summary="Generate business insights",
    description="Generate comprehensive insights from customer segments"
)
async def generate_insights(
    request: InsightRequest,
    db: Session = Depends(get_db)
):
    """
    Generate business insights from customer segmentation data.

    - **dataset_id**: ID of the dataset
    """
    # Get dataset
    dataset = DatasetService.get_dataset_by_id(db, request.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {request.dataset_id} not found"
        )

    # Generate insights
    result = InsightService.generate_segment_insights(db, request.dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': result['message'],
        'data': result['data']
    }


@router.get(
    "/filtered-insights/{dataset_id}",
    summary="Generate insights with filters",
    description="Generate insights filtered by segment and/or date range"
)
async def get_filtered_insights(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter (e.g., 'High Value,Loyal')"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Generate business insights for a filtered subset of customer segments.

    - **dataset_id**: ID of the dataset
    - **segment**: Optional comma-separated segment filter
    - **date_from**: Optional start date (YYYY-MM-DD)
    - **date_to**: Optional end date (YYYY-MM-DD)
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    output_dir = settings.processed_data_path
    df = InsightService.load_segments_data(output_dir)

    if df is None or df.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No segment data found. Please run segmentation first."
        )

    try:
        # Parse and apply segment filter
        segments: Optional[List[str]] = None
        if segment:
            segments = [s.strip() for s in segment.split(',') if s.strip()]
            if segments:
                df = df[df['segment'].isin(segments)]

        # Apply date filter
        date_col = next((c for c in ['invoicedate', 'invoice_date', 'date', 'order_date'] if c in df.columns), None)
        if date_col and (date_from or date_to):
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            if date_from:
                df = df[df[date_col] >= pd.Timestamp(date_from)]
            if date_to:
                df = df[df[date_col] <= pd.Timestamp(date_to)]

        if df.empty:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data matches the specified filters."
            )

        # Compute insights on filtered data
        top_revenue = InsightService.compute_top_revenue_segment(df)
        largest = InsightService.compute_largest_segment(df)
        at_risk = InsightService.compute_at_risk_volume(df)
        avg_values = InsightService.compute_avg_order_value_by_segment(df)
        opportunities = InsightService.identify_growth_opportunities(df)

        total_revenue = round(df['monetary'].sum(), 2) if 'monetary' in df.columns else 0
        segment_distribution = {str(k): int(v) for k, v in df['segment'].value_counts().to_dict().items()}

        return {
            'success': True,
            'message': 'Filtered insights generated successfully',
            'data': {
                'filters_applied': {
                    'segments': segments or [],
                    'date_from': date_from,
                    'date_to': date_to
                },
                'filtered_customers': len(df),
                'total_revenue': total_revenue,
                'segment_distribution': segment_distribution,
                'top_revenue_segment': top_revenue,
                'largest_segment': largest,
                'at_risk_metrics': at_risk,
                'segment_value_analysis': avg_values,
                'growth_opportunities': opportunities,
                'timestamp': datetime.utcnow().isoformat()
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating filtered insights: {str(e)}"
        )


@router.get(
    "/insights-summary/{dataset_id}",
    summary="Get insights summary",
    description="Fetch key metrics and insights for a dataset"
)
async def get_insights_summary(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get insights summary with key metrics.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = InsightService.generate_segment_insights(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': result['message'],
        'data': result['data']
    }


@router.get(
    "/segment-revenue-analysis/{dataset_id}",
    summary="Get segment revenue analysis",
    description="Detailed revenue metrics by segment"
)
async def get_revenue_analysis(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed revenue analysis by segment.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    output_dir = settings.processed_data_path
    df = InsightService.load_segments_data(output_dir)

    if df is None or df.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No segment data found"
        )

    try:
        avg_values = InsightService.compute_avg_order_value_by_segment(df)

        return {
            'success': True,
            'message': 'Revenue analysis retrieved successfully',
            'data': {
                'total_revenue': round(df['monetary'].sum(), 2) if 'monetary' in df.columns else 0,
                'segment_analysis': avg_values,
                'timestamp': datetime.utcnow().isoformat()
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error analyzing revenue: {str(e)}"
        )


@router.get(
    "/at-risk-customers/{dataset_id}",
    summary="Get at-risk customer metrics",
    description="Fetch at-risk customer volume and impact analysis"
)
async def get_at_risk_metrics(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get at-risk customer metrics and recovery potential.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    output_dir = settings.processed_data_path
    df = InsightService.load_segments_data(output_dir)

    if df is None or df.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No segment data found"
        )

    try:
        at_risk_metrics = InsightService.compute_at_risk_volume(df)

        return {
            'success': True,
            'message': 'At-risk metrics retrieved successfully',
            'data': {
                'at_risk_analysis': at_risk_metrics,
                'timestamp': datetime.utcnow().isoformat()
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error analyzing at-risk customers: {str(e)}"
        )


@router.get(
    "/growth-opportunities/{dataset_id}",
    summary="Get growth opportunity signals",
    description="Identify growth opportunities and strategic recommendations"
)
async def get_growth_opportunities(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get growth opportunity signals and strategic recommendations.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    output_dir = settings.processed_data_path
    df = InsightService.load_segments_data(output_dir)

    if df is None or df.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No segment data found"
        )

    try:
        opportunities = InsightService.identify_growth_opportunities(df)

        return {
            'success': True,
            'message': 'Growth opportunities identified successfully',
            'data': {
                'opportunities': opportunities,
                'opportunity_count': len(opportunities),
                'timestamp': datetime.utcnow().isoformat()
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error identifying opportunities: {str(e)}"
        )
