"""
Dashboard Response Schemas
Pydantic models for dashboard API responses
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class MetricCard(BaseModel):
    """Single KPI metric card"""
    label: str = Field(..., description="Label for the metric")
    value: str = Field(..., description="Value to display (formatted)")
    icon: str = Field(..., description="Icon name (e.g., 'users', 'dollar-sign')")
    color: str = Field(..., description="Color theme (e.g., 'blue', 'green')")

    class Config:
        json_schema_extra = {
            "example": {
                "label": "Total Customers",
                "value": "5432",
                "icon": "users",
                "color": "blue"
            }
        }


class KPISummary(BaseModel):
    """KPI summary with aggregate metrics"""
    total_customers: int = Field(..., description="Total number of customers")
    total_revenue: float = Field(..., description="Total revenue in dollars")
    avg_customer_value: float = Field(..., description="Average customer lifetime value")
    healthy_percentage: float = Field(..., description="Percentage of healthy customers (High Value + Loyal)")
    at_risk_percentage: float = Field(..., description="Percentage of at-risk customers")
    new_customers: int = Field(..., description="Count of new customers")
    metric_cards: List[MetricCard] = Field(..., description="Individual KPI cards for dashboard display")

    class Config:
        json_schema_extra = {
            "example": {
                "total_customers": 5432,
                "total_revenue": 1250000.50,
                "avg_customer_value": 230.12,
                "healthy_percentage": 65.5,
                "at_risk_percentage": 12.3,
                "new_customers": 456,
                "metric_cards": []
            }
        }


class SegmentMetrics(BaseModel):
    """Metrics for a single segment"""
    total_revenue: float = Field(..., description="Total revenue from segment")
    customer_count: int = Field(..., description="Number of customers in segment")
    avg_revenue: float = Field(..., description="Average revenue per customer")
    percentage: float = Field(..., description="Percentage of total revenue")

    class Config:
        json_schema_extra = {
            "example": {
                "total_revenue": 450000.00,
                "customer_count": 250,
                "avg_revenue": 1800.00,
                "percentage": 36.0
            }
        }


class ChartDataset(BaseModel):
    """Dataset for chart visualization"""
    label: str = Field(..., description="Dataset label")
    data: List[Any] = Field(..., description="Data points for the dataset")
    backgroundColor: Optional[List[str]] = Field(None, description="Background colors for bars/areas")
    borderColor: Optional[str] = Field(None, description="Border color for the dataset")
    borderWidth: Optional[int] = Field(None, description="Border width in pixels")
    fill: Optional[bool] = Field(None, description="Whether to fill area under line")
    tension: Optional[float] = Field(None, description="Tension for curved lines (0-1)")
    pointRadius: Optional[int] = Field(None, description="Radius of data points")

    class Config:
        json_schema_extra = {
            "example": {
                "label": "High Value",
                "data": [100, 150, 200, 250],
                "backgroundColor": "#10B981",
                "borderColor": "#10B981",
                "borderWidth": 2
            }
        }


class ChartData(BaseModel):
    """Chart data structure for visualization"""
    labels: List[str] = Field(..., description="Labels for chart axes/categories")
    datasets: List[ChartDataset] = Field(..., description="Datasets to plot")
    title: Optional[str] = Field(None, description="Chart title")

    class Config:
        json_schema_extra = {
            "example": {
                "labels": ["Jan", "Feb", "Mar"],
                "datasets": [],
                "title": "Sample Chart"
            }
        }


class RevenueBySegmentResponse(BaseModel):
    """Revenue by segment response"""
    chart: ChartData = Field(..., description="Chart data for visualization")
    detailed: Dict[str, SegmentMetrics] = Field(..., description="Detailed metrics per segment")
    total_revenue: float = Field(..., description="Total revenue across all segments")

    class Config:
        json_schema_extra = {
            "example": {
                "chart": {"labels": [], "datasets": []},
                "detailed": {},
                "total_revenue": 1250000.00
            }
        }


class SegmentShareMetrics(BaseModel):
    """Metrics for segment share"""
    customer_count: int = Field(..., description="Number of customers in segment")
    percentage: float = Field(..., description="Percentage of total customers")

    class Config:
        json_schema_extra = {
            "example": {
                "customer_count": 250,
                "percentage": 25.5
            }
        }


class SegmentShareResponse(BaseModel):
    """Segment share distribution response"""
    chart: ChartData = Field(..., description="Pie chart data")
    detailed: Dict[str, SegmentShareMetrics] = Field(..., description="Detailed metrics per segment")
    total_customers: int = Field(..., description="Total customer count")

    class Config:
        json_schema_extra = {
            "example": {
                "chart": {"labels": [], "datasets": []},
                "detailed": {},
                "total_customers": 5432
            }
        }


class ScatterPoint(BaseModel):
    """Single point in scatter plot"""
    x: int = Field(..., description="X-axis value (frequency)")
    y: float = Field(..., description="Y-axis value (monetary)")
    label: str = Field(..., description="Point label (customer ID)")

    class Config:
        json_schema_extra = {
            "example": {
                "x": 5,
                "y": 1500.50,
                "label": "Customer 123"
            }
        }


class ScatterDataset(BaseModel):
    """Scatter plot dataset"""
    label: str = Field(..., description="Segment name")
    data: List[ScatterPoint] = Field(..., description="Data points")
    backgroundColor: str = Field(..., description="Point color")
    borderColor: str = Field(..., description="Point border color")
    pointRadius: int = Field(default=5, description="Point radius in pixels")
    pointHoverRadius: int = Field(default=7, description="Point hover radius in pixels")

    class Config:
        json_schema_extra = {
            "example": {
                "label": "High Value",
                "data": [],
                "backgroundColor": "#10B981",
                "borderColor": "#10B981",
                "pointRadius": 5,
                "pointHoverRadius": 7
            }
        }


class ScatterAxes(BaseModel):
    """Scatter plot axes configuration"""
    xAxisLabel: str = Field(..., description="X-axis label")
    yAxisLabel: str = Field(..., description="Y-axis label")

    class Config:
        json_schema_extra = {
            "example": {
                "xAxisLabel": "Purchase Frequency",
                "yAxisLabel": "Monetary Value ($)"
            }
        }


class ScatterResponse(BaseModel):
    """Scatter plot response"""
    datasets: List[ScatterDataset] = Field(..., description="Data for each segment")
    axes: ScatterAxes = Field(..., description="Axes labels and configuration")
    title: str = Field(..., description="Chart title")

    class Config:
        json_schema_extra = {
            "example": {
                "datasets": [],
                "axes": {"xAxisLabel": "Frequency", "yAxisLabel": "Value"},
                "title": "Frequency vs Monetary Value"
            }
        }


class TrendLineDataset(BaseModel):
    """Trend line dataset"""
    label: str = Field(..., description="Segment name")
    data: List[float] = Field(..., description="Monthly data points")
    borderColor: str = Field(..., description="Line color")
    backgroundColor: str = Field(..., description="Fill color (with alpha)")
    tension: float = Field(default=0.4, description="Line tension (0-1)")
    fill: bool = Field(default=True, description="Whether to fill area under line")
    pointRadius: int = Field(default=4, description="Point radius in pixels")

    class Config:
        json_schema_extra = {
            "example": {
                "label": "High Value",
                "data": [10000.0, 11000.0, 12500.0],
                "borderColor": "#10B981",
                "backgroundColor": "#10B98120",
                "tension": 0.4,
                "fill": True,
                "pointRadius": 4
            }
        }


class TrendResponse(BaseModel):
    """Monthly trend response"""
    labels: List[str] = Field(..., description="Month labels (YYYY-MM)")
    datasets: List[TrendLineDataset] = Field(..., description="Trend line for each segment")
    title: str = Field(..., description="Chart title")

    class Config:
        json_schema_extra = {
            "example": {
                "labels": ["2025-01", "2025-02"],
                "datasets": [],
                "title": "Monthly Revenue Trend"
            }
        }


class DashboardData(BaseModel):
    """Complete dashboard data"""
    timestamp: str = Field(..., description="ISO timestamp of data generation")
    kpis: KPISummary = Field(..., description="KPI cards and summary metrics")
    revenue_by_segment: RevenueBySegmentResponse = Field(..., description="Revenue analysis by segment")
    segment_share: SegmentShareResponse = Field(..., description="Segment distribution")
    scatter_data: ScatterResponse = Field(..., description="Frequency vs monetary scatter plot")
    monthly_trends: TrendResponse = Field(..., description="Monthly revenue trends")

    class Config:
        json_schema_extra = {
            "example": {
                "timestamp": "2026-05-13T15:30:00",
                "kpis": {},
                "revenue_by_segment": {},
                "segment_share": {},
                "scatter_data": {},
                "monthly_trends": {}
            }
        }


class DashboardResponse(BaseModel):
    """Standard dashboard response envelope"""
    success: bool = Field(..., description="Whether request was successful")
    message: str = Field(..., description="Response message")
    data: Optional[Any] = Field(None, description="Response data")
    timestamp: Optional[str] = Field(None, description="ISO timestamp of response")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Dashboard data retrieved successfully",
                "data": {},
                "timestamp": "2026-05-13T15:30:00"
            }
        }


class KPIResponse(BaseModel):
    """KPI-specific response"""
    success: bool = Field(..., description="Whether request was successful")
    message: str = Field(..., description="Response message")
    data: KPISummary = Field(..., description="KPI summary data")


class ChartResponse(BaseModel):
    """Chart data response"""
    success: bool = Field(..., description="Whether request was successful")
    message: str = Field(..., description="Response message")
    data: ChartData = Field(..., description="Chart data")
