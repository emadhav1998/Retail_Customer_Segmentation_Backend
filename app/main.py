"""
Main FastAPI application
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.routes import (
    health_router,
    dataset_router,
    exploration_router,
    cleaning_router,
    rfm_router,
    segmentation_router,
    dashboard_router,
    insights_router
)


# Create FastAPI app instance
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
    description="API for Retail Customer Segmentation Analysis"
)


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Register routers
app.include_router(health_router, prefix=settings.api_v1_prefix)
app.include_router(dataset_router, prefix=settings.api_v1_prefix)
app.include_router(exploration_router, prefix=settings.api_v1_prefix)
app.include_router(cleaning_router, prefix=settings.api_v1_prefix)
app.include_router(rfm_router, prefix=settings.api_v1_prefix)
app.include_router(segmentation_router, prefix=settings.api_v1_prefix)
app.include_router(dashboard_router, prefix=settings.api_v1_prefix)
app.include_router(insights_router, prefix=settings.api_v1_prefix)


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": f"Welcome to {settings.app_name}",
        "version": settings.app_version,
        "api_docs": "/docs"
    }


# Startup event - Initialize database tables
@app.on_event("startup")
async def startup_event():
    """Initialize database on application startup"""
    from app.db.session import init_db
    init_db()
    print(f"Database initialized: {settings.database_url}")


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown"""
    from app.db.session import engine
    engine.dispose()
    print("Database connections closed")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )
