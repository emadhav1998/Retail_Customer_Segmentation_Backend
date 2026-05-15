"""
Dashboard Service
Prepares data for dashboard visualization
"""
import os
import pandas as pd
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.dataset_service import DatasetService


class DashboardService:
    """Service for preparing dashboard visualization data"""

    @staticmethod
    def load_segments_data(output_dir: str) -> Optional[pd.DataFrame]:
        """Load customer segments CSV."""
        try:
            segment_file = os.path.join(output_dir, 'customer_segments.csv')
            if os.path.exists(segment_file):
                return pd.read_csv(segment_file, encoding='utf-8')
            return None
        except Exception:
            return None

    @staticmethod
    def prepare_kpi_cards(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Prepare KPI card data for dashboard header with comprehensive aggregation.

        Args:
            df: DataFrame with customer segments

        Returns:
            Dictionary with KPI metrics
        """
        if df is None or df.empty:
            return {
                'total_customers': 0,
                'total_revenue': 0.0,
                'avg_customer_value': 0.0,
                'healthy_percentage': 0.0,
                'at_risk_percentage': 0.0,
                'new_customers': 0,
                'metric_cards': []
            }

        try:
            # Validate required columns
            if 'monetary' not in df.columns or 'segment' not in df.columns:
                raise ValueError("Missing required columns: monetary, segment")

            # Core aggregations
            total_customers = len(df)
            total_revenue = float(df['monetary'].sum())
            avg_customer_value = total_revenue / total_customers if total_customers > 0 else 0.0
            
            # Segment-based calculations
            segment_counts = df['segment'].value_counts()
            
            # Healthy customers: High Value + Loyal (predictable, stable revenue)
            healthy_segments = ['High Value', 'Loyal']
            healthy_customers = sum(segment_counts.get(seg, 0) for seg in healthy_segments)
            health_percentage = (healthy_customers / total_customers * 100) if total_customers > 0 else 0.0
            
            # At Risk customers: potential churn risk
            at_risk_count = segment_counts.get('At Risk', 0)
            at_risk_percentage = (at_risk_count / total_customers * 100) if total_customers > 0 else 0.0
            
            # New customers: growth potential
            new_count = segment_counts.get('New', 0)
            
            # Low engagement: recovery potential
            low_engagement_count = segment_counts.get('Low Engagement', 0)
            
            # Calculate revenue metrics by segment
            if 'segment' in df.columns:
                segment_revenue = df.groupby('segment')['monetary'].sum()
                high_value_revenue = segment_revenue.get('High Value', 0)
                high_value_percentage = (high_value_revenue / total_revenue * 100) if total_revenue > 0 else 0.0
            else:
                high_value_percentage = 0.0
            
            # Recency-based metrics (if available)
            if 'recency' in df.columns:
                avg_recency = float(df['recency'].mean())
                max_recency = int(df['recency'].max())
            else:
                avg_recency = 0.0
                max_recency = 0

            metric_cards = [
                {
                    'label': 'Total Customers',
                    'value': f"{total_customers:,}",
                    'icon': 'users',
                    'color': 'blue'
                },
                {
                    'label': 'Total Revenue',
                    'value': f"${total_revenue:,.0f}",
                    'icon': 'dollar-sign',
                    'color': 'green'
                },
                {
                    'label': 'Avg Customer Value',
                    'value': f"${avg_customer_value:,.0f}",
                    'icon': 'trending-up',
                    'color': 'purple'
                },
                {
                    'label': 'Healthy %',
                    'value': f"{health_percentage:.1f}%",
                    'icon': 'heart',
                    'color': 'emerald'
                },
                {
                    'label': 'At Risk %',
                    'value': f"{at_risk_percentage:.1f}%",
                    'icon': 'alert-circle',
                    'color': 'red'
                }
            ]

            return {
                'total_customers': int(total_customers),
                'total_revenue': round(total_revenue, 2),
                'avg_customer_value': round(avg_customer_value, 2),
                'healthy_percentage': round(health_percentage, 1),
                'at_risk_percentage': round(at_risk_percentage, 1),
                'new_customers': int(new_count),
                'low_engagement_customers': int(low_engagement_count),
                'high_value_revenue_percentage': round(high_value_percentage, 1),
                'avg_recency_days': round(avg_recency, 1),
                'max_recency_days': max_recency,
                'metric_cards': metric_cards
            }
        except Exception as e:
            # Return empty KPIs on error rather than crashing
            return {
                'total_customers': 0,
                'total_revenue': 0.0,
                'avg_customer_value': 0.0,
                'healthy_percentage': 0.0,
                'at_risk_percentage': 0.0,
                'new_customers': 0,
                'metric_cards': [],
                'error': str(e)
            }

    @staticmethod
    def prepare_revenue_by_segment(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Prepare revenue by segment data for bar chart.

        Args:
            df: DataFrame with customer segments

        Returns:
            Dictionary with segment revenue data
        """
        if df is None or df.empty or 'segment' not in df.columns or 'monetary' not in df.columns:
            return {}

        try:
            segment_revenue = df.groupby('segment')['monetary'].agg(['sum', 'count', 'mean']).reset_index()
            segment_revenue.columns = ['segment', 'total_revenue', 'customer_count', 'avg_revenue']
            segment_revenue = segment_revenue.sort_values('total_revenue', ascending=False)

            # Format for chart
            chart_data = {
                'labels': segment_revenue['segment'].tolist(),
                'datasets': [
                    {
                        'label': 'Total Revenue',
                        'data': [round(x, 2) for x in segment_revenue['total_revenue']],
                        'backgroundColor': ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#6366F1'][:len(segment_revenue)],
                        'borderColor': '#fff',
                        'borderWidth': 2
                    }
                ]
            }

            # Detailed metrics
            detailed = {}
            for _, row in segment_revenue.iterrows():
                detailed[row['segment']] = {
                    'total_revenue': round(row['total_revenue'], 2),
                    'customer_count': int(row['customer_count']),
                    'avg_revenue': round(row['avg_revenue'], 2),
                    'percentage': round((row['total_revenue'] / segment_revenue['total_revenue'].sum() * 100), 1)
                }

            return {
                'chart': chart_data,
                'detailed': detailed,
                'total_revenue': round(segment_revenue['total_revenue'].sum(), 2)
            }
        except Exception:
            return {}

    @staticmethod
    def prepare_segment_share(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Prepare segment share data for pie chart.

        Args:
            df: DataFrame with customer segments

        Returns:
            Dictionary with segment distribution data
        """
        if df is None or df.empty or 'segment' not in df.columns:
            return {}

        try:
            segment_counts = df['segment'].value_counts().reset_index()
            segment_counts.columns = ['segment', 'count']
            segment_counts['percentage'] = (segment_counts['count'] / segment_counts['count'].sum() * 100)
            segment_counts = segment_counts.sort_values('count', ascending=False)

            # Format for pie chart
            colors = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#6366F1']
            chart_data = {
                'labels': segment_counts['segment'].tolist(),
                'datasets': [
                    {
                        'label': 'Customer Count',
                        'data': segment_counts['count'].tolist(),
                        'backgroundColor': colors[:len(segment_counts)],
                        'borderColor': '#fff',
                        'borderWidth': 2
                    }
                ]
            }

            # Detailed breakdown
            detailed = {}
            for _, row in segment_counts.iterrows():
                detailed[row['segment']] = {
                    'customer_count': int(row['count']),
                    'percentage': round(row['percentage'], 1)
                }

            return {
                'chart': chart_data,
                'detailed': detailed,
                'total_customers': int(segment_counts['count'].sum())
            }
        except Exception:
            return {}

    @staticmethod
    def prepare_scatter_data(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Prepare frequency vs monetary scatter plot data.

        Args:
            df: DataFrame with customer segments

        Returns:
            Dictionary with scatter plot data
        """
        if df is None or df.empty or 'frequency' not in df.columns or 'monetary' not in df.columns:
            return {}

        try:
            # Prepare scatter data by segment
            segments = df['segment'].unique()
            
            scatter_datasets = []
            colors_map = {
                'High Value': '#10B981',
                'Loyal': '#3B82F6',
                'New': '#F59E0B',
                'At Risk': '#EF4444',
                'Low Engagement': '#6366F1',
                'Other': '#8B5CF6'
            }

            for segment in sorted(segments):
                segment_data = df[df['segment'] == segment]
                points = [
                    {
                        'x': int(row['frequency']),
                        'y': round(row['monetary'], 2),
                        'label': row.get('customerid', f"Customer {i}")
                    }
                    for i, (_, row) in enumerate(segment_data.iterrows())
                ]

                scatter_datasets.append({
                    'label': segment,
                    'data': points,
                    'backgroundColor': colors_map.get(segment, '#8B5CF6'),
                    'borderColor': colors_map.get(segment, '#8B5CF6'),
                    'pointRadius': 5,
                    'pointHoverRadius': 7
                })

            return {
                'datasets': scatter_datasets,
                'axes': {
                    'xAxisLabel': 'Purchase Frequency',
                    'yAxisLabel': 'Monetary Value ($)'
                },
                'title': 'Customer Frequency vs Monetary Value by Segment'
            }
        except Exception:
            return {}

    @staticmethod
    def prepare_monthly_trends(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Prepare monthly trend data for line chart.

        Args:
            df: DataFrame with customer segments

        Returns:
            Dictionary with monthly trend data
        """
        if df is None or df.empty:
            return {}

        try:
            # Check if we have a date column (if so, use it; otherwise simulate)
            if 'invoicedate' in df.columns:
                df['invoicedate'] = pd.to_datetime(df['invoicedate'], errors='coerce')
                df['year_month'] = df['invoicedate'].dt.to_period('M')
                
                # Group by month and segment
                monthly_data = df.groupby(['year_month', 'segment']).agg({
                    'customerid': 'count',
                    'monetary': 'sum'
                }).reset_index()
                monthly_data.columns = ['year_month', 'segment', 'customer_count', 'revenue']
                monthly_data['year_month'] = monthly_data['year_month'].astype(str)
            else:
                # Simulate monthly trends based on recency
                # Create synthetic months (last 12 months)
                today = datetime.now()
                months = [(today - timedelta(days=30*i)).strftime('%Y-%m') for i in range(11, -1, -1)]
                
                # Distribute customers across months based on recency
                monthly_data_list = []
                for month in months:
                    for segment in df['segment'].unique():
                        segment_df = df[df['segment'] == segment]
                        customer_count = len(segment_df) // 12 + (1 if pd.Timestamp.now().month == int(month.split('-')[1]) else 0)
                        revenue = segment_df['monetary'].sum() / 12
                        monthly_data_list.append({
                            'year_month': month,
                            'segment': segment,
                            'customer_count': customer_count,
                            'revenue': revenue
                        })
                
                monthly_data = pd.DataFrame(monthly_data_list)

            # Prepare chart data by segment
            segments = sorted(monthly_data['segment'].unique())
            months = sorted(monthly_data['year_month'].unique())
            
            colors_map = {
                'High Value': '#10B981',
                'Loyal': '#3B82F6',
                'New': '#F59E0B',
                'At Risk': '#EF4444',
                'Low Engagement': '#6366F1',
                'Other': '#8B5CF6'
            }

            datasets = []
            for segment in segments:
                segment_monthly = monthly_data[monthly_data['segment'] == segment]
                revenue_by_month = [
                    round(segment_monthly[segment_monthly['year_month'] == month]['revenue'].sum(), 2)
                    for month in months
                ]
                
                datasets.append({
                    'label': segment,
                    'data': revenue_by_month,
                    'borderColor': colors_map.get(segment, '#8B5CF6'),
                    'backgroundColor': colors_map.get(segment, '#8B5CF6') + '20',
                    'tension': 0.4,
                    'fill': True,
                    'pointRadius': 4
                })

            return {
                'labels': months,
                'datasets': datasets,
                'title': 'Monthly Revenue Trend by Segment'
            }
        except Exception:
            return {}

    @staticmethod
    def apply_filters(
        df: pd.DataFrame,
        segments: Optional[List[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Apply segment and date filters to the dataframe before aggregation.

        Args:
            df: DataFrame with customer segments
            segments: Optional list of segment names to include
            date_from: Optional start date string (YYYY-MM-DD)
            date_to: Optional end date string (YYYY-MM-DD)

        Returns:
            Filtered DataFrame
        """
        filtered = df.copy()

        # Filter by segment names
        if segments and len(segments) > 0:
            valid_segments = [s.strip() for s in segments if s.strip()]
            if valid_segments:
                filtered = filtered[filtered['segment'].isin(valid_segments)]

        # Filter by date range (requires invoicedate or similar date column)
        date_col = None
        for col in ['invoicedate', 'invoice_date', 'date', 'order_date']:
            if col in filtered.columns:
                date_col = col
                break

        if date_col and (date_from or date_to):
            filtered[date_col] = pd.to_datetime(filtered[date_col], errors='coerce')
            if date_from:
                try:
                    filtered = filtered[filtered[date_col] >= pd.Timestamp(date_from)]
                except Exception:
                    pass
            if date_to:
                try:
                    filtered = filtered[filtered[date_col] <= pd.Timestamp(date_to)]
                except Exception:
                    pass

        return filtered

    @staticmethod
    def get_dashboard_data(
        db: Session,
        dataset_id: int,
        segments: Optional[List[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get complete dashboard data with optional filters.

        Args:
            db: Database session
            dataset_id: ID of the dataset
            segments: Optional list of segments to include
            date_from: Optional start date (YYYY-MM-DD)
            date_to: Optional end date (YYYY-MM-DD)

        Returns:
            Dictionary with all dashboard sections
        """
        output_dir = settings.processed_data_path
        df = DashboardService.load_segments_data(output_dir)

        if df is None or df.empty:
            return {
                'success': False,
                'message': 'No segment data found. Please run segmentation first.',
                'data': None
            }

        # Apply filters before aggregation
        filters_applied = bool(segments or date_from or date_to)
        if filters_applied:
            df = DashboardService.apply_filters(df, segments=segments, date_from=date_from, date_to=date_to)

        if df.empty:
            return {
                'success': False,
                'message': 'No data matches the specified filters.',
                'data': None
            }

        try:
            return {
                'success': True,
                'message': 'Dashboard data prepared successfully',
                'data': {
                    'timestamp': datetime.utcnow().isoformat(),
                    'filters_applied': filters_applied,
                    'active_filters': {
                        'segments': segments or [],
                        'date_from': date_from,
                        'date_to': date_to
                    },
                    'kpis': DashboardService.prepare_kpi_cards(df),
                    'revenue_by_segment': DashboardService.prepare_revenue_by_segment(df),
                    'segment_share': DashboardService.prepare_segment_share(df),
                    'scatter_data': DashboardService.prepare_scatter_data(df),
                    'monthly_trends': DashboardService.prepare_monthly_trends(df)
                }
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error preparing dashboard data: {str(e)}',
                'data': None
            }
