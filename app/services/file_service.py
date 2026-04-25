"""
File service for handling dataset uploads and validation
"""
import os
import uuid
from datetime import datetime
from typing import Optional, Tuple
from fastapi import UploadFile
from app.core.config import settings


class FileService:
    """Service for handling file operations"""
    
    # Allowed file extensions
    ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
    
    @staticmethod
    def validate_upload(file: UploadFile) -> Tuple[bool, Optional[str]]:
        """
        Validate uploaded file.
        
        Args:
            file: FastAPI UploadFile object
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check file extension
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in FileService.ALLOWED_EXTENSIONS:
            return False, f"Invalid file type. Allowed: {', '.join(FileService.ALLOWED_EXTENSIONS)}"
        
        # Check file size (read first chunk to estimate)
        # Note: In production, implement proper size checking
        return True, None
    
    @staticmethod
    def generate_save_path(original_filename: str) -> Tuple[str, str]:
        """
        Generate unique file path for saving uploaded file.
        
        Args:
            original_filename: Original name of the uploaded file
            
        Returns:
            Tuple of (file_path, file_name)
        """
        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        file_ext = os.path.splitext(original_filename)[1]
        new_filename = f"{timestamp}_{unique_id}{file_ext}"
        
        # Create full path
        save_dir = settings.raw_data_path
        os.makedirs(save_dir, exist_ok=True)
        file_path = os.path.join(save_dir, new_filename)
        
        return file_path, new_filename
    
    @staticmethod
    def check_dataset_exists(file_name: str) -> bool:
        """
        Check if dataset with given file name already exists.
        
        Args:
            file_name: Name of the file to check
            
        Returns:
            True if dataset exists, False otherwise
        """
        # This would query the database in a real implementation
        # For now, check if file exists in raw data directory
        file_path = os.path.join(settings.raw_data_path, file_name)
        return os.path.exists(file_path)
    
    @staticmethod
    async def save_uploaded_file(file: UploadFile, save_path: str) -> bool:
        """
        Save uploaded file to disk.
        
        Args:
            file: FastAPI UploadFile object
            save_path: Destination path for the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            with open(save_path, "wb") as f:
                content = await file.read()
                f.write(content)
            
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False
    
    @staticmethod
    def delete_file(file_path: str) -> bool:
        """
        Delete file from disk.
        
        Args:
            file_path: Path to the file to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
            return True
        except Exception as e:
            print(f"Error deleting file: {e}")
            return False
    
    @staticmethod
    def get_file_size(file_path: str) -> int:
        """
        Get file size in bytes.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File size in bytes, or 0 if file doesn't exist
        """
        try:
            return os.path.getsize(file_path)
        except Exception:
            return 0


# Create singleton instance
file_service = FileService()