"""Dashboard routes"""
from fastapi import APIRouter, status


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/overview", status_code=status.HTTP_200_OK)
async def get_dashboard_overview():
    """
    Get dashboard overview with key metrics
    """
    return {
        "message": "Dashboard overview placeholder"
    }


@router.get("/segment-distribution", status_code=status.HTTP_200_OK)
async def get_segment_distribution():
    """
    Get segment distribution statistics
    """
    return {
        "message": "Segment distribution placeholder"
    }


@router.get("/metrics", status_code=status.HTTP_200_OK)
async def get_metrics():
    """
    Get key performance metrics
    """
    return {
        "message": "Metrics placeholder"
    }
