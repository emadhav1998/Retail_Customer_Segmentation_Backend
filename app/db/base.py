"""
Base models for database models
"""
from datetime import datetime
from sqlalchemy import Column, DateTime
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class TimeStampedModel(Base):
    """Base model with timestamp columns"""
    __abstract__ = True
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


# Import all models to register them with Base.metadata
from app.models.dataset import Dataset
from app.models.job import Job
from app.models.segment import Segment

__all__ = ["Base", "TimeStampedModel", "Dataset", "Job", "Segment"]
