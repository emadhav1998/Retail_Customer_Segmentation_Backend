"""
Job model for tracking pipeline execution
"""
from sqlalchemy import Column, Integer, String, DateTime, Enum as SQLEnum, Text
from datetime import datetime
import enum
from app.db.base import Base


class JobStatus(str, enum.Enum):
    """Job execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobStage(str, enum.Enum):
    """Pipeline stages"""
    UPLOAD = "upload"
    PREPROCESSING = "preprocessing"
    RFM_CALCULATION = "rfm_calculation"
    CLUSTERING = "clustering"
    SEGMENT_MAPPING = "segment_mapping"
    INSIGHTS_GENERATION = "insights_generation"
    COMPLETE = "complete"


class Job(Base):
    """Pipeline job tracking model"""
    __tablename__ = "jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    stage = Column(SQLEnum(JobStage), default=JobStage.UPLOAD)
    status = Column(SQLEnum(JobStatus), default=JobStatus.PENDING)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    dataset_id = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    progress_percentage = Column(Integer, default=0)
    logs = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<Job(id={self.id}, name='{self.name}', stage='{self.stage}', status='{self.status}')>"