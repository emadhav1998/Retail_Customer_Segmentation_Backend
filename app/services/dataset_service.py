"""
Dataset service for handling dataset operations
"""
import os
import pandas as pd
from typing import Optional, Tuple, List
from datetime import datetime
from sqlalchemy.orm import Session

from app.models.dataset import Dataset, DatasetStatus, DatasetSourceType
from app.core.config import settings
from app.services.file_service import FileService


class DatasetService:
    """Service for managing datasets"""
    
    # Required columns for retail customer segmentation
    REQUIRED_COLUMNS = [
        "InvoiceNo",
        "StockCode", 
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country"
    ]
    
    # Optional but recommended columns
    OPTIONAL_COLUMNS = [
        "ProductCategory",
        "ProductGroup",
        "Discount",
        "ShippingCost"
    ]
    
    @staticmethod
    def save_raw_dataset(
        db: Session,
        file_content: bytes,
        original_filename: str,
        source_type: DatasetSourceType = DatasetSourceType.UPLOAD
    ) -> Tuple[Dataset, str]:
        """
        Step 1: Save raw dataset to disk and create database record.
        
        Args:
            db: Database session
            file_content: Raw file content bytes
            original_filename: Original uploaded filename
            source_type: Source type (upload/api/scraped)
            
        Returns:
            Tuple of (Dataset object, file_path)
        """
        # Generate unique file path
        file_path, file_name = FileService.generate_save_path(original_filename)
        
        # Save file to disk
        with open(file_path, 'wb') as f:
            f.write(file_content)
        
        # Create dataset record in database
        dataset = Dataset(
            file_name=file_name,
            original_file_name=original_filename,
            source_type=source_type,
            file_path=file_path,
            status=DatasetStatus.PENDING,
            row_count=0
        )
        
        db.add(dataset)
        db.commit()
        db.refresh(dataset)
        
        return dataset, file_path
    
    @staticmethod
    def read_headers(file_path: str) -> Tuple[List[str], int]:
        """
        Step 2: Read column headers from dataset.
        
        Args:
            file_path: Path to the dataset file
            
        Returns:
            Tuple of (list of column names, row count)
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        
        try:
            if file_ext == '.csv':
                # Read first few rows to get headers
                df = pd.read_csv(file_path, nrows=0)
                columns = df.columns.tolist()
                
                # Count rows
                row_count = sum(1 for _ in open(file_path, encoding='utf-8')) - 1  # Subtract header
                
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, nrows=0)
                columns = df.columns.tolist()
                
                # Count rows
                df_full = pd.read_excel(file_path)
                row_count = len(df_full)
            else:
                return [], 0
            
            return columns, row_count
            
        except Exception as e:
            print(f"Error reading headers: {e}")
            return [], 0
    
    @staticmethod
    def validate_required_columns(columns: List[str]) -> Tuple[bool, List[str], List[str]]:
        """
        Step 3: Validate that required columns are present.
        
        Args:
            columns: List of column names from the dataset
            
        Returns:
            Tuple of (is_valid, missing_columns, extra_columns)
        """
        columns_lower = [col.lower().strip() for col in columns]
        required_lower = [col.lower().strip() for col in DatasetService.REQUIRED_COLUMNS]
        
        # Find missing columns
        missing = []
        for req_col in required_lower:
            if req_col not in columns_lower:
                # Try to find similar column names
                found = False
                for col in columns_lower:
                    if req_col in col or col in req_col:
                        found = True
                        break
                if not found:
                    missing.append(req_col)
        
        # Find extra columns (optional)
        extra = [col for col in columns_lower if col not in required_lower]
        
        is_valid = len(missing) == 0
        
        return is_valid, missing, extra
    
    @staticmethod
    def create_dataset_record(
        db: Session,
        file_path: str,
        original_filename: str,
        source_type: DatasetSourceType = DatasetSourceType.UPLOAD
    ) -> Dataset:
        """
        Step 4: Create dataset record with validated metadata.
        
        Args:
            db: Database session
            file_path: Path to the saved file
            original_filename: Original filename
            source_type: Source type
            
        Returns:
            Dataset object
        """
        # Read headers and row count
        columns, row_count = DatasetService.read_headers(file_path)
        
        # Validate columns
        is_valid, missing, extra = DatasetService.validate_required_columns(columns)
        
        if not is_valid:
            # Create record with failed status
            dataset = Dataset(
                file_name=os.path.basename(file_path),
                original_file_name=original_filename,
                source_type=source_type,
                file_path=file_path,
                status=DatasetStatus.FAILED,
                row_count=row_count,
                error_message=f"Missing required columns: {', '.join(missing)}"
            )
        else:
            # Create record with pending status
            dataset = Dataset(
                file_name=os.path.basename(file_path),
                original_file_name=original_filename,
                source_type=source_type,
                file_path=file_path,
                status=DatasetStatus.PENDING,
                row_count=row_count
            )
        
        db.add(dataset)
        db.commit()
        db.refresh(dataset)
        
        return dataset
    
    @staticmethod
    def get_dataset_by_id(db: Session, dataset_id: int) -> Optional[Dataset]:
        """
        Get dataset by ID.
        
        Args:
            db: Database session
            dataset_id: Dataset ID
            
        Returns:
            Dataset object or None
        """
        return db.query(Dataset).filter(Dataset.id == dataset_id).first()
    
    @staticmethod
    def get_all_datasets(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        status: Optional[DatasetStatus] = None
    ) -> Tuple[List[Dataset], int]:
        """
        Get all datasets with optional filtering.
        
        Args:
            db: Database session
            skip: Number of records to skip
            limit: Maximum records to return
            status: Filter by status
            
        Returns:
            Tuple of (list of datasets, total count)
        """
        query = db.query(Dataset)
        
        if status:
            query = query.filter(Dataset.status == status)
        
        total = query.count()
        datasets = query.order_by(Dataset.upload_date.desc()).offset(skip).limit(limit).all()
        
        return datasets, total
    
    @staticmethod
    def update_dataset_status(
        db: Session,
        dataset_id: int,
        status: DatasetStatus,
        error_message: Optional[str] = None
    ) -> Optional[Dataset]:
        """
        Update dataset status.
        
        Args:
            db: Database session
            dataset_id: Dataset ID
            status: New status
            error_message: Error message if failed
            
        Returns:
            Updated Dataset object or None
        """
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if dataset:
            dataset.status = status
            if error_message:
                dataset.error_message = error_message
            if status == DatasetStatus.COMPLETED:
                dataset.processed_at = datetime.utcnow()
            
            db.commit()
            db.refresh(dataset)
        
        return dataset
    
    @staticmethod
    def delete_dataset(db: Session, dataset_id: int) -> bool:
        """
        Delete dataset and associated file.
        
        Args:
            db: Database session
            dataset_id: Dataset ID
            
        Returns:
            True if successful, False otherwise
        """
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if not dataset:
            return False
        
        # Delete file if exists
        if dataset.file_path and os.path.exists(dataset.file_path):
            try:
                os.remove(dataset.file_path)
            except Exception as e:
                print(f"Error deleting file: {e}")
        
        # Delete database record
        db.delete(dataset)
        db.commit()
        
        return True
    
    @staticmethod
    def get_dataset_columns(db: Session, dataset_id: int) -> Optional[List[str]]:
        """
        Get column names from a dataset.
        
        Args:
            db: Database session
            dataset_id: Dataset ID
            
        Returns:
            List of column names or None
        """
        dataset = DatasetService.get_dataset_by_id(db, dataset_id)
        
        if not dataset or not dataset.file_path:
            return None
        
        try:
            columns, _ = DatasetService.read_headers(dataset.file_path)
            return columns
        except Exception as e:
            print(f"Error reading columns: {e}")
            return None


# Create singleton instance
dataset_service = DatasetService()