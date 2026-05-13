"""
Segment model for storing customer segmentation results
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Enum as SQLEnum, Text
from datetime import datetime
import enum
from app.db.base import Base


class SegmentName(str, enum.Enum):
    """Customer segment names"""
    HIGH_VALUE = "High Value"
    LOYAL = "Loyal"
    NEW = "New"
    AT_RISK = "At Risk"
    LOW_ENGAGEMENT = "Low Engagement"
    OTHER = "Other"


class Segment(Base):
    """Customer segment results model"""
    __tablename__ = "segments"
    
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(String(100), nullable=False, index=True)
    segment_name = Column(SQLEnum(SegmentName), nullable=False, index=True)
    rfm_score = Column(String(3), nullable=False)  # e.g., "555"
    rfm_total = Column(Integer, nullable=False)  # numeric score e.g., 555
    recency_score = Column(Integer, nullable=False)
    frequency_score = Column(Integer, nullable=False)
    monetary_score = Column(Integer, nullable=False)
    recency_value = Column(Float, nullable=True)
    frequency_value = Column(Integer, nullable=True)
    monetary_value = Column(Float, nullable=True)
    cluster_id = Column(Integer, nullable=True)
    job_id = Column(Integer, nullable=True)
    segment_rank = Column(Integer, nullable=True)  # Rank of segment by importance (1 = most important)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<Segment(id={self.id}, customer_id='{self.customer_id}', segment='{self.segment_name}')>"