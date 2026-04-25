"""
Segment model for storing customer segmentation results
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Enum as SQLEnum
from datetime import datetime
import enum
from app.db.base import Base


class SegmentName(str, enum.Enum):
    """Customer segment names"""
    CHAMPIONS = "Champions"
    LOYAL_CUSTOMERS = "Loyal Customers"
    POTENTIAL_LOYALIST = "Potential Loyalist"
    NEW_CUSTOMERS = "New Customers"
    AT_RISK = "At Risk"
    HIBERNATING = "Hibernating"
    LOST = "Lost"
    CANT_LOOSE_THEM = "Can't LoseThem"
    NEED_ATTENTION = "Need Attention"
    ABOUT_TO_SLEEP = "About to Sleep"


class Segment(Base):
    """Customer segment results model"""
    __tablename__ = "segments"
    
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(String(100), nullable=False, index=True)
    segment_name = Column(SQLEnum(SegmentName), nullable=False)
    rfm_score = Column(Integer, nullable=False)
    recency_score = Column(Integer, nullable=False)
    frequency_score = Column(Integer, nullable=False)
    monetary_score = Column(Integer, nullable=False)
    recency_value = Column(Float, nullable=True)
    frequency_value = Column(Integer, nullable=True)
    monetary_value = Column(Float, nullable=True)
    cluster_id = Column(Integer, nullable=True)
    job_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<Segment(id={self.id}, customer_id='{self.customer_id}', segment='{self.segment_name}')>"