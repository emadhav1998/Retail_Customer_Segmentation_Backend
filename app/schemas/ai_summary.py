"""
AI Summary Request and Response Schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class SegmentMetricInput(BaseModel):
    """Metrics for a single segment used as input to AI summary"""
    segment: str = Field(..., description="Segment name")
    customer_count: int = Field(..., description="Number of customers in segment")
    total_revenue: float = Field(..., description="Total revenue from segment")
    avg_revenue: float = Field(..., description="Average revenue per customer")
    percentage_of_total: float = Field(..., description="Percentage of total customers")

    class Config:
        json_schema_extra = {
            "example": {
                "segment": "High Value",
                "customer_count": 300,
                "total_revenue": 450000.0,
                "avg_revenue": 1500.0,
                "percentage_of_total": 25.0
            }
        }


class AISummaryRequest(BaseModel):
    """Request model for AI summary generation"""
    dataset_id: int = Field(..., description="ID of the dataset")
    segment_metrics: Optional[List[SegmentMetricInput]] = Field(
        None,
        description="Optional pre-computed segment metrics. If omitted, will be read from customer_segments.csv"
    )
    include_recommendations: bool = Field(
        True,
        description="Whether to include actionable recommendations in the summary"
    )
    include_risk_analysis: bool = Field(
        True,
        description="Whether to include at-risk customer analysis"
    )
    include_growth_signals: bool = Field(
        True,
        description="Whether to include growth opportunity signals"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "dataset_id": 1,
                "include_recommendations": True,
                "include_risk_analysis": True,
                "include_growth_signals": True
            }
        }


class SegmentSummaryLine(BaseModel):
    """A single line of executive summary for a segment"""
    segment: str = Field(..., description="Segment name")
    headline: str = Field(..., description="Key headline for the segment")
    detail: str = Field(..., description="Detailed description")
    action: str = Field(..., description="Recommended action")

    class Config:
        json_schema_extra = {
            "example": {
                "segment": "High Value",
                "headline": "Top revenue drivers — protect at all costs",
                "detail": "High Value customers contribute 40% of revenue with only 15% of base",
                "action": "Launch VIP loyalty program and exclusive upsell offers"
            }
        }


class RiskSummary(BaseModel):
    """Risk analysis summary"""
    at_risk_count: int = Field(..., description="Number of at-risk customers")
    at_risk_revenue: float = Field(..., description="Revenue at risk")
    recovery_potential: float = Field(..., description="Estimated recoverable revenue")
    urgency: str = Field(..., description="Risk urgency level (High/Medium/Low)")
    recommended_action: str = Field(..., description="Top recommended action")

    class Config:
        json_schema_extra = {
            "example": {
                "at_risk_count": 120,
                "at_risk_revenue": 85000.0,
                "recovery_potential": 25500.0,
                "urgency": "High",
                "recommended_action": "Launch win-back campaign within 7 days"
            }
        }


class GrowthSignal(BaseModel):
    """A single growth opportunity signal"""
    opportunity: str = Field(..., description="Opportunity description")
    segment: str = Field(..., description="Target segment")
    estimated_uplift: float = Field(..., description="Estimated revenue uplift")
    action: str = Field(..., description="Recommended action")

    class Config:
        json_schema_extra = {
            "example": {
                "opportunity": "Convert New customers to Loyal",
                "segment": "New",
                "estimated_uplift": 45000.0,
                "action": "Deploy first-repeat-purchase incentive sequence"
            }
        }


class AISummaryData(BaseModel):
    """Complete AI summary output"""
    timestamp: str = Field(..., description="ISO timestamp of summary generation")
    dataset_id: int = Field(..., description="Dataset ID")
    total_customers: int = Field(..., description="Total customers analyzed")
    total_revenue: float = Field(..., description="Total revenue in dataset")
    executive_summary: str = Field(..., description="Full executive summary text")
    executive_bullets: List[str] = Field(..., description="Key bullet points")
    segment_summaries: List[SegmentSummaryLine] = Field(..., description="Per-segment summary lines")
    risk_summary: Optional[RiskSummary] = Field(None, description="Risk analysis summary")
    growth_signals: Optional[List[GrowthSignal]] = Field(None, description="Growth opportunity signals")
    recommendations: Optional[List[str]] = Field(None, description="Prioritized recommendations")

    class Config:
        json_schema_extra = {
            "example": {
                "timestamp": "2026-05-15T12:00:00",
                "dataset_id": 1,
                "total_customers": 1000,
                "total_revenue": 1250000.0,
                "executive_summary": "The customer base is dominated by Loyal...",
                "executive_bullets": [],
                "segment_summaries": [],
                "risk_summary": None,
                "growth_signals": [],
                "recommendations": []
            }
        }


class AISummaryResponse(BaseModel):
    """Standard response envelope for AI summary endpoints"""
    success: bool = Field(..., description="Whether the request succeeded")
    message: str = Field(..., description="Response message")
    data: Optional[AISummaryData] = Field(None, description="Summary data")
    timestamp: Optional[str] = Field(None, description="ISO response timestamp")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "AI summary generated successfully",
                "data": {},
                "timestamp": "2026-05-15T12:00:00"
            }
        }
