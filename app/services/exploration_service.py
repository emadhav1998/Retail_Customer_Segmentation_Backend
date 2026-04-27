"""
Exploration Service
Handles data exploration workflow and job management
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


class ExplorationService:
    """Service for managing data exploration operations"""
    
    @staticmethod
    def trigger_exploration(
        db: Session,
        dataset_id: int,
        job_id: Optional[int] = None
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Step 1: Trigger data exploration for a dataset.
        
        Args:
            db: Database session
            dataset_id: ID of the dataset to explore
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
            # Import and run exploration
            from scripts.preprocessing.explore_data import run_exploration
            
            # Update job status if provided
            if job_id:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = JobStatus.RUNNING
                    job.stage = JobStage.PREPROCESSING
                    job.started_at = datetime.utcnow()
                    job.progress_percentage = 10
                    db.commit()
            
            # Run exploration
            output_path, summary = run_exploration(
                file_path=dataset.file_path,
                output_dir=settings.processed_data_path
            )
            
            # Update job status
            if job_id:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = JobStatus.COMPLETED
                    job.stage = JobStage.PREPROCESSING
                    job.completed_at = datetime.utcnow()
                    job.progress_percentage = 100
                    job.logs = f"Exploration completed. Summary saved to {output_path}"
                    db.commit()
            
            return True, f"Exploration completed successfully. Summary saved to {output_path}", summary
            
        except Exception as e:
            error_message = str(e)
            
            # Update job status on failure
            if job_id:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = JobStatus.FAILED
                    job.error_message = error_message
                    db.commit()
            
            return False, f"Exploration failed: {error_message}", None
    
    @staticmethod
    def save_summary_metrics(
        db: Session,
        dataset_id: int,
        summary: Dict[str, Any]
    ) -> str:
        """
        Step 2: Save exploration summary metrics to file.
        
        Args:
            db: Database session
            dataset_id: Dataset ID
            summary: Summary dictionary from exploration
            
        Returns:
            Path to saved summary file
        """
        # Create output directory
        output_dir = settings.processed_data_path
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"exploration_dataset_{dataset_id}_{timestamp}.json"
        output_path = os.path.join(output_dir, filename)
        
        # Add metadata
        summary_with_metadata = {
            "dataset_id": dataset_id,
            "exploration_timestamp": datetime.now().isoformat(),
            "summary": summary
        }
        
        # Save to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary_with_metadata, f, indent=2, default=str)
        
        return output_path
    
    @staticmethod
    def update_job_status(
        db: Session,
        job_id: int,
        status: JobStatus,
        progress: int = 0,
        error_message: Optional[str] = None,
        logs: Optional[str] = None
    ) -> Optional[Job]:
        """
        Step 3: Update job status for exploration progress.
        
        Args:
            db: Database session
            job_id: Job ID
            status: New job status
            progress: Progress percentage (0-100)
            error_message: Error message if failed
            logs: Additional logs
            
        Returns:
            Updated Job object or None
        """
        job = db.query(Job).filter(Job.id == job_id).first()
        
        if not job:
            return None
        
        job.status = status
        job.progress_percentage = progress
        
        if error_message:
            job.error_message = error_message
        
        if logs:
            job.logs = logs
        
        if status == JobStatus.RUNNING and not job.started_at:
            job.started_at = datetime.utcnow()
        
        if status in [JobStatus.COMPLETED, JobStatus.FAILED]:
            job.completed_at = datetime.utcnow()
        
        db.commit()
        db.refresh(job)
        
        return job
    
    @staticmethod
    def get_exploration_summary(
        db: Session,
        dataset_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get exploration summary for a dataset.
        
        Args:
            db: Database session
            dataset_id: Dataset ID
            
        Returns:
            Summary dictionary or None
        """
        # Look for existing exploration summary
        output_dir = settings.processed_data_path
        
        if not os.path.exists(output_dir):
            return None
        
        # Find the latest summary file for this dataset
        summary_files = [
            f for f in os.listdir(output_dir)
            if f.startswith(f"exploration_dataset_{dataset_id}_")
        ]
        
        if not summary_files:
            return None
        
        # Get the most recent file
        latest_file = sorted(summary_files)[-1]
        file_path = os.path.join(output_dir, latest_file)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("summary")
        except Exception as e:
            print(f"Error reading summary: {e}")
            return None
    
    @staticmethod
    def get_exploration_summary_file(
        db: Session,
        dataset_id: int
    ) -> Optional[str]:
        """
        Get path to exploration summary file.
        
        Args:
            db: Database session
            dataset_id: Dataset ID
            
        Returns:
            Path to summary file or None
        """
        output_dir = settings.processed_data_path
        
        if not os.path.exists(output_dir):
            return None
        
        summary_files = [
            f for f in os.listdir(output_dir)
            if f.startswith(f"exploration_dataset_{dataset_id}_")
        ]
        
        if not summary_files:
            return None
        
        latest_file = sorted(summary_files)[-1]
        return os.path.join(output_dir, latest_file)


# Create singleton instance
exploration_service = ExplorationService()