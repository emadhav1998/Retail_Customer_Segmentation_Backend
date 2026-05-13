"""
Clustering Service
Handles K-Means clustering workflow
"""
import os
import json
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from app.models.dataset import Dataset
from app.models.job import Job, JobStatus, JobStage
from app.core.config import settings
from app.services.dataset_service import DatasetService


class ClusteringService:
    """Service for managing K-Means clustering operations"""

    @staticmethod
    def run_clustering(
        db: Session,
        dataset_id: int,
        job_id: Optional[int] = None,
        n_clusters: Optional[int] = None
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Run K-Means clustering on customer data.

        Args:
            db: Database session
            dataset_id: ID of the dataset
            job_id: Optional job ID for tracking
            n_clusters: Number of clusters (defaults to settings.n_clusters)

        Returns:
            Tuple of (success, message, summary_dict)
        """
        # Get dataset
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)

        if not dataset:
            return False, f"Dataset with ID {dataset_id} not found", None

        # Use default from settings if not provided
        if n_clusters is None:
            n_clusters = settings.n_clusters

        # Update job status if provided
        if job_id:
            job = db.query(Job).filter(Job.id == job_id).first()
            if job:
                job.status = JobStatus.IN_PROGRESS
                job.stage = JobStage.CLUSTERING
                db.commit()

        try:
            # Import the clustering function
            from scripts.segmentation.clustering import run_clustering

            output_dir = settings.processed_data_path

            # Check if customer_segments.csv exists (output from segmentation)
            # If not, fall back to rfm_scores.csv
            input_file = os.path.join(output_dir, 'customer_segments.csv')
            if not os.path.exists(input_file):
                input_file = os.path.join(output_dir, 'rfm_scores.csv')

            if not os.path.exists(input_file):
                return False, f"Input file not found. Please complete segmentation first.", None

            # Run clustering
            success, message, summary = run_clustering(
                input_file,
                output_dir,
                n_clusters=n_clusters,
                random_state=settings.random_state
            )

            if not success:
                if job_id:
                    job = db.query(Job).filter(Job.id == job_id).first()
                    if job:
                        job.status = JobStatus.FAILED
                        job.error_message = message
                        db.commit()
                return False, message, None

            # Update job status to completed
            if job_id:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = JobStatus.COMPLETED
                    job.completed_at = datetime.utcnow()
                    db.commit()

            # Extract cluster summary
            cluster_metrics = summary.get('cluster_metrics', {})
            
            return True, message, {
                'n_clusters': summary.get('n_clusters'),
                'total_customers': summary.get('total_customers'),
                'cluster_counts': {
                    cluster_id: metrics.get('customer_count', 0)
                    for cluster_id, metrics in cluster_metrics.items()
                },
                'cluster_metrics': cluster_metrics,
                'kmeans_inertia': summary.get('kmeans_inertia'),
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            error_msg = f"Error during clustering: {str(e)}"
            if job_id:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = JobStatus.FAILED
                    job.error_message = error_msg
                    db.commit()
            return False, error_msg, None

    @staticmethod
    def get_clustering_summary(db: Session, dataset_id: int) -> Optional[Dict[str, Any]]:
        """
        Get clustering summary for a dataset.

        Args:
            db: Database session
            dataset_id: ID of the dataset

        Returns:
            Dictionary with clustering metrics or None if not found
        """
        try:
            output_dir = settings.processed_data_path
            summary_file = os.path.join(output_dir, 'clustering_summary.json')

            if not os.path.exists(summary_file):
                return None

            with open(summary_file, 'r') as f:
                summary = json.load(f)

            return summary

        except Exception:
            return None
