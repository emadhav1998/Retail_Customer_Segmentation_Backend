"""Health check routes"""
from fastapi import APIRouter, status, Depends
from sqlalchemy.orm import Session
from datetime import datetime
from app.db.session import get_db, engine
from app.core.config import settings


router = APIRouter(tags=["health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint to verify API is running and database is accessible.
    """
    # Check database connection
    db_status = "healthy"
    try:
        # Test database connection
        connection = engine.connect()
        connection.close()
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "message": "API is running",
        "database": db_status,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/", status_code=status.HTTP_200_OK)
async def root():
    """Root endpoint"""
    return {
        "message": "Retail Customer Segmentation API",
        "version": settings.app_version,
        "docs": "/docs"
    }


@router.get("/ready", status_code=status.HTTP_200_OK)
async def readiness_check():
    """
    Readiness check endpoint for Kubernetes/load balancer.
    """
    return {
        "ready": True,
        "service": settings.app_name,
        "version": settings.app_version
    }
