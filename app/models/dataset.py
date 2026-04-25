"""
Dataset model for storing dataset metadata
"""
from sqlalchemy import Column, Integer, String, DateTime, Enum as SQLEnum
from datetime import datetime
import enum
from app.db.base import Base


class DatasetStatus(str, enum.Enum):
    """Dataset processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class DatasetSourceType(str, enum.Enum):
    """Source type for datasets"""
    UPLOAD = "upload"
    API = "api"
    SCRAPED = "scraped"


class Dataset(Base):
    """Dataset metadata model"""
    __tablename__ = "datasets"
    
    id = Column(Integer, primary_key=True, index=True)
    file_name = Column(String(255), nullable=False)
    original_file_name = Column(String(255), nullable=True)
    upload_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    source_type = Column(SQLEnum(DatasetSourceType), default=DatasetSourceType.UPLOAD)
    row_count = Column(Integer, default=0)
    status = Column(SQLEnum(DatasetStatus), default=DatasetStatus.PENDING)
    file_path = Column(String(500), nullable=True)
    error_message = Column(String(1000), nullable=True)
    processed_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<Dataset(id={self.id}, file_name='{self.file_name}', status='{self.status}')>"