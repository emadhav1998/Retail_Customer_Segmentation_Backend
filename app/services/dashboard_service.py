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
        Prepare KPI card data for dashboard header.

        Args:
            df: DataFrame with customer segments

        Returns:
            Dictionary with KPI metrics
        """
        if df is None or df.empty:
            return {}

        try:
            total_customers = len(df)
            total_revenue = float(df['monetary'].sum()) if 'monetary' in df.columns else 0
            avg_customer_value = total_revenue / total_customers if total_customers > 0 else 0
            
            # Calculate segment distribution
            segment_counts = df['segment'].value_counts()
            
            # High Value and Loyal are "healthy" customers
            healthy_customers = (
                segment_counts.get('High Value', 0) + 
                segment_counts.get('Loyal', 0)
            )
            health_percentage = (healthy_customers / total_customers * 100) if total_customers > 0 else 0
            
            # At Risk represents risk
            at_risk_count = segment_counts.get('At Risk', 0)
            at_risk_percentage = (at_risk_count / total_customers * 100) if total_customers > 0 else 0

            # New represents growth potential
            new_count = segment_counts.get('New', 0)
            
            return {
                'total_customers': int(total_customers),
                'total_revenue': round(total_revenue, 2),
                'avg_customer_value': round(avg_customer_value, 2),
                'healthy_percentage': round(health_percentage, 1),
                'at_risk_percentage': round(at_risk_percentage, 1),
                'new_customers': int(new_count),
                'metric_cards': [
                    {
                        'label': 'Total Customers',
                        'value': int(total_customers),
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
                        'color': 'green'
                    },
                    {
                        'label': 'At Risk %',
                        'value': f"{at_risk_percentage:.1f}%",
                        'icon': 'alert-circle',
                        'color': 'red'
                    }
                ]
            }
        except Exception:
            return {}

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
    def get_dashboard_data(db: Session, dataset_id: int) -> Dict[str, Any]:
        """
        Get complete dashboard data.

        Args:
            db: Database session
            dataset_id: ID of the dataset

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

        try:
            return {
                'success': True,
                'message': 'Dashboard data prepared successfully',
                'data': {
                    'timestamp': datetime.utcnow().isoformat(),
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
