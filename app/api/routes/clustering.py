"""
Clustering API Routes
Endpoints for K-Means clustering analysis
"""
from fastapi import APIRouter, status, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
import os
import pandas as pd

from app.db.session import get_db
from app.models.dataset import Dataset
from app.models.job import Job, JobStatus, JobStage
from app.services.clustering_service import ClusteringService
from app.services.dataset_service import DatasetService
from app.core.config import settings


router = APIRouter(prefix="/clustering", tags=["clustering"])


class ClusteringRequest(BaseModel):
    """Request model for clustering"""
    dataset_id: int = Field(..., description="ID of the dataset")
    n_clusters: Optional[int] = Field(None, description="Number of clusters (defaults to settings.n_clusters)")


class ClusteringResponse(BaseModel):
    """Response model for clustering"""
    success: bool
    message: str
    job_id: Optional[int] = None
    data: Optional[Dict[str, Any]] = None


@router.post(
    "/run-clustering",
    summary="Run K-Means clustering",
    description="Execute K-Means clustering on scaled RFM metrics",
    response_model=ClusteringResponse
)
async def run_clustering(
    request: ClusteringRequest,
    db: Session = Depends(get_db)
):
    """
    Run K-Means clustering on customer data.

    - **dataset_id**: ID of the dataset
    - **n_clusters**: Number of clusters (optional, defaults to settings)
    """
    # Get dataset
    dataset = DatasetService.get_dataset_by_id(db, request.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {request.dataset_id} not found"
        )

    # Create job
    job = Job(
        name=f"clustering_{request.dataset_id}",
        stage=JobStage.CLUSTERING,
        status=JobStatus.PENDING,
        dataset_id=request.dataset_id
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run clustering
    success, message, summary = ClusteringService.run_clustering(
        db,
        request.dataset_id,
        job_id=job.id,
        n_clusters=request.n_clusters
    )

    if not success:
        return {
            'success': False,
            'message': message,
            'job_id': job.id,
            'data': None
        }

    return {
        'success': True,
        'message': message,
        'job_id': job.id,
        'data': summary
    }


@router.get(
    "/clustering-summary/{dataset_id}",
    summary="Get clustering summary",
    description="Fetch clustering metrics and cluster statistics"
)
async def get_clustering_summary(
    dataset_id: int,
    db: Session = Depends(get_db)
):
    """
    Get clustering summary with cluster counts and centroid information.

    - **dataset_id**: ID of the dataset
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    # Get clustering summary from file
    cluster_file = os.path.join(settings.processed_data_path, 'customer_clusters.csv')

    if not os.path.exists(cluster_file):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No clustering results found. Please run /run-clustering first."
        )

    try:
        df = pd.read_csv(cluster_file, encoding='utf-8')

        # Calculate cluster statistics
        cluster_counts = df['cluster_id'].value_counts().sort_index().to_dict()
        total_customers = len(df)

        # Detailed cluster metrics
        cluster_metrics = {}
        for cluster_id in sorted(df['cluster_id'].unique()):
            cluster_data = df[df['cluster_id'] == cluster_id]
            
            cluster_metrics[cluster_id] = {
                'customer_count': len(cluster_data),
                'percentage': round((len(cluster_data) / total_customers * 100), 2),
                'cluster_label': cluster_data['cluster_label'].iloc[0] if 'cluster_label' in df.columns else f"Cluster_{cluster_id}",
                'avg_recency': round(cluster_data['recency'].mean(), 2) if 'recency' in df.columns else None,
                'avg_frequency': round(cluster_data['frequency'].mean(), 2) if 'frequency' in df.columns else None,
                'avg_monetary': round(cluster_data['monetary'].mean(), 2) if 'monetary' in df.columns else None,
                'min_recency': round(cluster_data['recency'].min(), 2) if 'recency' in df.columns else None,
                'max_recency': round(cluster_data['recency'].max(), 2) if 'recency' in df.columns else None,
                'min_frequency': int(cluster_data['frequency'].min()) if 'frequency' in df.columns else None,
                'max_frequency': int(cluster_data['frequency'].max()) if 'frequency' in df.columns else None,
                'min_monetary': round(cluster_data['monetary'].min(), 2) if 'monetary' in df.columns else None,
                'max_monetary': round(cluster_data['monetary'].max(), 2) if 'monetary' in df.columns else None,
            }

        return {
            'success': True,
            'message': 'Clustering summary retrieved successfully',
            'data': {
                'total_customers': total_customers,
                'n_clusters': len(cluster_counts),
                'cluster_counts': cluster_counts,
                'cluster_metrics': cluster_metrics,
                'timestamp': datetime.utcnow().isoformat()
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading clustering data: {str(e)}"
        )


@router.get(
    "/cluster-details/{dataset_id}/{cluster_id}",
    summary="Get cluster customer details",
    description="Fetch detailed information about customers in a specific cluster"
)
async def get_cluster_details(
    dataset_id: int,
    cluster_id: int,
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get detailed customer information for a specific cluster.

    - **dataset_id**: ID of the dataset
    - **cluster_id**: ID of the cluster
    - **limit**: Maximum number of customers to return (default 100)
    """
    dataset = DatasetService.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with ID {dataset_id} not found"
        )

    cluster_file = os.path.join(settings.processed_data_path, 'customer_clusters.csv')

    if not os.path.exists(cluster_file):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No clustering results found."
        )

    try:
        df = pd.read_csv(cluster_file, encoding='utf-8')
        
        cluster_data = df[df['cluster_id'] == cluster_id].head(limit)

        if cluster_data.empty:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cluster {cluster_id} not found or is empty"
            )

        # Convert to records
        records = cluster_data.to_dict('records')

        return {
            'success': True,
            'message': f'Retrieved {len(records)} customers from cluster {cluster_id}',
            'data': {
                'cluster_id': cluster_id,
                'customer_count': len(records),
                'customers': records,
                'timestamp': datetime.utcnow().isoformat()
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading cluster details: {str(e)}"
        )
