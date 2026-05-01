"""
RFM Service
Handles RFM (Recency, Frequency, Monetary) analysis workflow
"""
import os
import json
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from app.models.dataset import Dataset, DatasetStatus
from app.models.job import Job, JobStatus, JobStage
from app.core.config import settings
from app.services.dataset_service import DatasetService


class RFMService:
    """Service for managing RFM calculations"""
    
    @staticmethod
    def calculate_recency(
        db: Session,
        dataset_id: int,
        job_id: Optional[int] = None,
        reference_date: Optional[str] = None
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Calculate recency metrics for a dataset.
        
        Args:
            db: Database session
            dataset_id: ID of the dataset
            job_id: Optional job ID for tracking
            reference_date: Optional reference date (YYYY-MM-DD format)
            
        Returns:
            Tuple of (success, message, summary_dict)
        """
        # Get dataset
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if not dataset:
            return False, f"Dataset with ID {dataset_id} not found", None
        
        # Get the cleaned_final.csv file path
        output_dir = settings.processed_data_path
        input_file = os.path.join(output_dir, 'cleaned_final.csv')
        
        if not os.path.exists(input_file):
            return False, f"Cleaned final file not found. Please complete cleaning step 2 first.", None
        
        try:
            # Import and run recency calculation script
            from scripts.segmentation.calculate_recency import run_recency_calculation
            
            # Update job status if provided
            if job_id:
                RFMService._update_job_status(db, job_id, JobStatus.IN_PROGRESS, 10, "Starting recency calculation...")
            
            # Run recency calculation
            success, message, summary = run_recency_calculation(
                input_file=input_file,
                output_dir=output_dir,
                reference_date=reference_date,
                save_summary=True
            )
            
            if success and summary:
                # Update job progress
                if job_id:
                    RFMService._update_job_status(db, job_id, JobStatus.IN_PROGRESS, 90, "Saving reference date metadata...")
                
                # Store reference date metadata
                RFMService._store_reference_date_metadata(
                    db=db,
                    dataset_id=dataset_id,
                    job_id=job_id,
                    summary=summary
                )
                
                # Update job completion
                if job_id:
                    RFMService._update_job_status(
                        db, job_id, JobStatus.COMPLETED, 100, 
                        "Recency calculation completed successfully"
                    )
                
                return True, message, summary
            else:
                if job_id:
                    RFMService._update_job_status(
                        db, job_id, JobStatus.FAILED, 0, 
                        f"Recency calculation failed: {message}"
                    )
                return False, message, None
                
        except Exception as e:
            error_msg = f"Error during recency calculation: {str(e)}"
            
            if job_id:
                RFMService._update_job_status(
                    db, job_id, JobStatus.FAILED, 0, error_msg
                )
            
            return False, error_msg, None
    
    @staticmethod
    def _update_job_status(
        db: Session,
        job_id: int,
        status: JobStatus,
        progress: int,
        message: str
    ) -> None:
        """Update job status and progress."""
        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.status = status
            job.progress_percentage = progress
            job.logs = (job.logs or "") + f"\n[{datetime.now().isoformat()}] {message}"
            if status == JobStatus.COMPLETED or status == JobStatus.FAILED:
                job.completed_at = datetime.now()
            db.commit()
    
    @staticmethod
    def _store_reference_date_metadata(
        db: Session,
        dataset_id: int,
        job_id: Optional[int],
        summary: Dict[str, Any]
    ) -> None:
        """Store reference date and recency metadata in dataset record."""
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            # Update dataset status
            dataset.status = DatasetStatus.PROCESSED
            dataset.processed_at = datetime.now()
            db.commit()
    
    @staticmethod
    def get_recency_preview(
        db: Session,
        dataset_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get recency preview/metadata for a dataset.
        
        Args:
            db: Database session
            dataset_id: ID of the dataset
            
        Returns:
            Dictionary with recency metadata or None
        """
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if not dataset:
            return None
        
        # Look for recency summary file
        output_dir = settings.processed_data_path
        summary_file = os.path.join(output_dir, 'recency_calculation_summary.json')
        
        if os.path.exists(summary_file):
            with open(summary_file, 'r') as f:
                return json.load(f)
        
        return None
    
    @staticmethod
    def get_rfm_job_status(
        db: Session,
        job_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get RFM job status.
        
        Args:
            db: Database session
            job_id: ID of the job
            
        Returns:
            Dictionary with job status or None
        """
        job = db.query(Job).filter(Job.id == job_id).first()
        
        if not job:
            return None
        
        return {
            'job_id': job.id,
            'name': job.name,
            'stage': job.stage.value,
            'status': job.status.value,
            'progress_percentage': job.progress_percentage,
            'logs': job.logs,
            'started_at': job.started_at.isoformat() if job.started_at else None,
            'completed_at': job.completed_at.isoformat() if job.completed_at else None,
            'error_message': job.error_message,
            'dataset_id': job.dataset_id
        }
    
    @staticmethod
    def get_customer_recency_data(
        db: Session,
        dataset_id: int,
        limit: Optional[int] = None
    ) -> Optional[list]:
        """
        Get customer recency data preview.
        
        Args:
            db: Database session
            dataset_id: ID of the dataset
            limit: Optional limit on number of records
            
        Returns:
            List of customer recency records or None
        """
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if not dataset:
            return None
        
        # Read recency CSV file
        output_dir = settings.processed_data_path
        recency_file = os.path.join(output_dir, 'rfm_recency.csv')
        
        if not os.path.exists(recency_file):
            return None
        
        try:
            import pandas as pd
            
            df = pd.read_csv(recency_file)
            
            if limit:
                df = df.head(limit)
            
            # Convert to list of dictionaries
            return df.to_dict('records')
        
        except Exception as e:
            return None