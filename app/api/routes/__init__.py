"""API routes module initialization"""
from .health import router as health_router
from .dataset import router as dataset_router
from .exploration import router as exploration_router
from .cleaning import router as cleaning_router
from .rfm import router as rfm_router
from .segmentation import router as segmentation_router
from .clustering import router as clustering_router
from .dashboard import router as dashboard_router
from .insights import router as insights_router

__all__ = [
    "health_router",
    "dataset_router",
    "exploration_router",
    "cleaning_router",
    "rfm_router",
    "segmentation_router",
    "clustering_router",
    "dashboard_router",
    "insights_router"
]
