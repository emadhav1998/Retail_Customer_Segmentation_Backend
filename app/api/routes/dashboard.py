"""
Dashboard API Routes
Endpoints for dashboard data visualization
"""
from fastapi import APIRouter, status, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field

from app.db.session import get_db
from app.models.dataset import Dataset
from app.services.dashboard_service import DashboardService
from app.services.dataset_service import DatasetService
from app.schemas.dashboard import DashboardResponse


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


class DashboardRequest(BaseModel):
    """Request model for dashboard data"""
    dataset_id: int = Field(..., description="ID of the dataset")


def parse_segments(segment: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated segment string into a list."""
    if not segment:
        return None
    return [s.strip() for s in segment.split(',') if s.strip()]


@router.get(
    "/overview/{dataset_id}",
    summary="Get dashboard overview",
    description="Fetch KPI cards and overview metrics",
    response_model=DashboardResponse
)
async def get_dashboard_overview(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter (e.g., 'High Value,Loyal')"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get dashboard overview with KPI cards and key metrics.

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

    result = DashboardService.get_dashboard_data(db, dataset_id, segments=parse_segments(segment), date_from=date_from, date_to=date_to)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Dashboard overview retrieved successfully',
        'data': result['data'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/kpi-summary/{dataset_id}",
    summary="Get KPI summary",
    description="Fetch key performance indicator cards",
    response_model=DashboardResponse
)
async def get_kpi_summary(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get KPI summary with dashboard cards.

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

    result = DashboardService.get_dashboard_data(db, dataset_id, segments=parse_segments(segment), date_from=date_from, date_to=date_to)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'KPI summary retrieved successfully',
        'data': result['data']['kpis'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/revenue-by-segment/{dataset_id}",
    summary="Get revenue by segment",
    description="Fetch revenue distribution across segments",
    response_model=DashboardResponse
)
async def get_revenue_by_segment(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get revenue analysis by segment for bar chart.

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

    result = DashboardService.get_dashboard_data(db, dataset_id, segments=parse_segments(segment), date_from=date_from, date_to=date_to)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Revenue by segment retrieved successfully',
        'data': result['data']['revenue_by_segment'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/segment-share/{dataset_id}",
    summary="Get segment share",
    description="Fetch segment distribution percentages",
    response_model=DashboardResponse
)
async def get_segment_share(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get segment share distribution for pie chart.

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

    result = DashboardService.get_dashboard_data(db, dataset_id, segments=parse_segments(segment), date_from=date_from, date_to=date_to)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Segment share retrieved successfully',
        'data': result['data']['segment_share'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/scatter-data/{dataset_id}",
    summary="Get scatter plot data",
    description="Fetch frequency vs monetary value scatter data",
    response_model=DashboardResponse
)
async def get_scatter_data(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get frequency vs monetary scatter plot data.

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

    result = DashboardService.get_dashboard_data(db, dataset_id, segments=parse_segments(segment), date_from=date_from, date_to=date_to)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Scatter data retrieved successfully',
        'data': result['data']['scatter_data'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/monthly-trends/{dataset_id}",
    summary="Get monthly trend data",
    description="Fetch monthly revenue trends by segment",
    response_model=DashboardResponse
)
async def get_monthly_trends(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get monthly trend data for line chart.

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

    result = DashboardService.get_dashboard_data(db, dataset_id, segments=parse_segments(segment), date_from=date_from, date_to=date_to)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Monthly trends retrieved successfully',
        'data': result['data']['monthly_trends'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.post(
    "/full-dashboard/{dataset_id}",
    summary="Get complete dashboard",
    description="Fetch all dashboard sections at once",
    response_model=DashboardResponse
)
async def get_full_dashboard(
    dataset_id: int,
    segment: Optional[str] = Query(None, description="Comma-separated segment names to filter"),
    date_from: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get complete dashboard with all sections.

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

    result = DashboardService.get_dashboard_data(db, dataset_id, segments=parse_segments(segment), date_from=date_from, date_to=date_to)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Full dashboard retrieved successfully',
        'data': result['data'],
        'timestamp': datetime.utcnow().isoformat()
    }



@router.get(
    "/overview/{dataset_id}",
    summary="Get dashboard overview",
    description="Fetch KPI cards and overview metrics",
    response_model=DashboardResponse
)
async def get_dashboard_overview(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get dashboard overview with KPI cards and key metrics.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = DashboardService.get_dashboard_data(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Dashboard overview retrieved successfully',
        'data': result['data'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/kpi-summary/{dataset_id}",
    summary="Get KPI summary",
    description="Fetch key performance indicator cards",
    response_model=DashboardResponse
)
async def get_kpi_summary(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get KPI summary with dashboard cards.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = DashboardService.get_dashboard_data(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'KPI summary retrieved successfully',
        'data': result['data']['kpis'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/revenue-by-segment/{dataset_id}",
    summary="Get revenue by segment",
    description="Fetch revenue distribution across segments",
    response_model=DashboardResponse
)
async def get_revenue_by_segment(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get revenue analysis by segment for bar chart.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = DashboardService.get_dashboard_data(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Revenue by segment retrieved successfully',
        'data': result['data']['revenue_by_segment'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/segment-share/{dataset_id}",
    summary="Get segment share",
    description="Fetch segment distribution percentages",
    response_model=DashboardResponse
)
async def get_segment_share(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get segment share distribution for pie chart.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = DashboardService.get_dashboard_data(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Segment share retrieved successfully',
        'data': result['data']['segment_share'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/scatter-data/{dataset_id}",
    summary="Get scatter plot data",
    description="Fetch frequency vs monetary value scatter data",
    response_model=DashboardResponse
)
async def get_scatter_data(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get frequency vs monetary scatter plot data.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = DashboardService.get_dashboard_data(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Scatter data retrieved successfully',
        'data': result['data']['scatter_data'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.get(
    "/monthly-trends/{dataset_id}",
    summary="Get monthly trend data",
    description="Fetch monthly revenue trends by segment",
    response_model=DashboardResponse
)
async def get_monthly_trends(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get monthly trend data for line chart.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = DashboardService.get_dashboard_data(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Monthly trends retrieved successfully',
        'data': result['data']['monthly_trends'],
        'timestamp': datetime.utcnow().isoformat()
    }


@router.post(
    "/full-dashboard/{dataset_id}",
    summary="Get complete dashboard",
    description="Fetch all dashboard sections at once",
    response_model=DashboardResponse
)
async def get_full_dashboard(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get complete dashboard with all sections.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    result = DashboardService.get_dashboard_data(db, dataset_id)

    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result['message']
        )

    return {
        'success': True,
        'message': 'Full dashboard retrieved successfully',
        'data': result['data'],
        'timestamp': datetime.utcnow().isoformat()
    }
