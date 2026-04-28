"""
Cleaning Service
Handles data cleaning workflow and job management
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


class CleaningService:
    """Service for managing data cleaning operations"""
    
    @staticmethod
    def run_cleaning_step1(
        db: Session,
        dataset_id: int,
        job_id: Optional[int] = None
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Run cleaning step 1 for a dataset.
        
        Args:
            db: Database session
            dataset_id: ID of the dataset to clean
            job_id: Optional job ID for tracking
            
        Returns:
            Tuple of (success, message, summary_dict)
        """
        # Get dataset
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if not dataset:
            return False, f"Dataset with ID {dataset_id} not found", None
        
        if not dataset.file_path or not os.path.exists(dataset.file_path):
            return False, f"Dataset file not found at {dataset.file_path}", None
        
        try:
            # Import and run cleaning script
            from scripts.preprocessing.clean_data_step1 import run_cleaning_step1
            
            # Update job status if provided
            if job_id:
                CleaningService._update_job_status(db, job_id, JobStatus.IN_PROGRESS, 10, "Starting cleaning step 1...")
            
            # Get output directory (processed data folder)
            output_dir = settings.processed_data_path
            
            # Run cleaning
            success, message, summary = run_cleaning_step1(
                input_file=dataset.file_path,
                output_dir=output_dir,
                save_summary=True
            )
            
            if success and summary:
                # Update job progress
                if job_id:
                    CleaningService._update_job_status(db, job_id, JobStatus.IN_PROGRESS, 90, "Saving output metadata...")
                
                # Store output metadata
                CleaningService._store_output_metadata(
                    db=db,
                    dataset_id=dataset_id,
                    job_id=job_id,
                    output_file=summary.get('output_file'),
                    summary=summary
                )
                
                # Update job completion
                if job_id:
                    CleaningService._update_job_status(
                        db, job_id, JobStatus.COMPLETED, 100, 
                        "Cleaning step 1 completed successfully"
                    )
                
                return True, message, summary
            else:
                if job_id:
                    CleaningService._update_job_status(
                        db, job_id, JobStatus.FAILED, 0, 
                        f"Cleaning failed: {message}"
                    )
                return False, message, None
                
        except Exception as e:
            error_msg = f"Error during cleaning: {str(e)}"
            
            if job_id:
                CleaningService._update_job_status(
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
    def _store_output_metadata(
        db: Session,
        dataset_id: int,
        job_id: Optional[int],
        output_file: str,
        summary: Dict[str, Any]
    ) -> None:
        """Store cleaning output metadata in dataset record."""
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            # Store the output file path in a custom field or create metadata
            # For now, we'll store it in the processed_at field as JSON
            metadata = {
                'output_file': output_file,
                'step': 'cleaning_step1',
                'summary': summary
            }
            # Update dataset status
            dataset.status = DatasetStatus.PROCESSED
            dataset.processed_at = datetime.now()
            db.commit()
    
    @staticmethod
    def get_cleaning_output_metadata(
        db: Session,
        dataset_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get cleaning output metadata for a dataset.
        
        Args:
            db: Database session
            dataset_id: ID of the dataset
            
        Returns:
            Dictionary with metadata or None
        """
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if not dataset:
            return None
        
        # Look for cleaning summary file
        output_dir = settings.processed_data_path
        summary_file = os.path.join(output_dir, 'cleaning_step1_summary.json')
        
        if os.path.exists(summary_file):
            with open(summary_file, 'r') as f:
                return json.load(f)
        
        return None
    
    @staticmethod
    def get_cleaning_status(
        db: Session,
        job_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get cleaning job status.
        
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
            'status': job.status.value,
            'progress_percentage': job.progress_percentage,
            'logs': job.logs,
            'started_at': job.started_at.isoformat() if job.started_at else None,
            'completed_at': job.completed_at.isoformat() if job.completed_at else None,
            'error_message': job.error_message
        }