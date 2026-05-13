"""
Insight Service
Generates business insights from customer segments and clusters
"""
import os
import pandas as pd
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.dataset_service import DatasetService


class InsightService:
    """Service for generating business insights from segmentation data"""

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
    def compute_top_revenue_segment(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute the segment with highest total revenue.

        Args:
            df: DataFrame with segment and monetary columns

        Returns:
            Dictionary with top revenue segment info
        """
        if 'segment' not in df.columns or 'monetary' not in df.columns:
            return {}

        segment_revenue = df.groupby('segment')['monetary'].sum().sort_values(ascending=False)

        if segment_revenue.empty:
            return {}

        top_segment = segment_revenue.index[0]
        top_revenue = float(segment_revenue.iloc[0])
        percentage = round((top_revenue / df['monetary'].sum() * 100), 2)

        return {
            'segment': top_segment,
            'total_revenue': round(top_revenue, 2),
            'percentage_of_total': percentage,
            'customer_count': len(df[df['segment'] == top_segment])
        }

    @staticmethod
    def compute_largest_segment(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute the largest segment by customer count.

        Args:
            df: DataFrame with segment column

        Returns:
            Dictionary with largest segment info
        """
        if 'segment' not in df.columns:
            return {}

        segment_counts = df['segment'].value_counts()

        if segment_counts.empty:
            return {}

        largest_segment = segment_counts.index[0]
        count = int(segment_counts.iloc[0])
        percentage = round((count / len(df) * 100), 2)
        avg_revenue = round(df[df['segment'] == largest_segment]['monetary'].mean(), 2) if 'monetary' in df.columns else 0

        return {
            'segment': largest_segment,
            'customer_count': count,
            'percentage_of_total': percentage,
            'avg_customer_value': avg_revenue
        }

    @staticmethod
    def compute_at_risk_volume(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute at-risk customer volume and potential impact.

        Args:
            df: DataFrame with segment and recency/monetary columns

        Returns:
            Dictionary with at-risk metrics
        """
        if 'segment' not in df.columns:
            return {}

        at_risk = df[df['segment'] == 'At Risk']

        if at_risk.empty:
            return {
                'at_risk_count': 0,
                'percentage_of_total': 0,
                'total_at_risk_revenue': 0,
                'avg_customer_value': 0,
                'recovery_potential': 0
            }

        count = len(at_risk)
        percentage = round((count / len(df) * 100), 2)
        total_revenue = round(at_risk['monetary'].sum(), 2) if 'monetary' in at_risk.columns else 0
        avg_value = round(at_risk['monetary'].mean(), 2) if 'monetary' in at_risk.columns else 0
        recovery_potential = round(total_revenue * 0.3, 2)  # Assume 30% recovery potential

        return {
            'at_risk_count': int(count),
            'percentage_of_total': percentage,
            'total_at_risk_revenue': total_revenue,
            'avg_customer_value': avg_value,
            'recovery_potential': recovery_potential
        }

    @staticmethod
    def compute_avg_order_value_by_segment(df: pd.DataFrame) -> Dict[str, float]:
        """
        Compute average order/monetary value by segment.

        Args:
            df: DataFrame with segment and monetary columns

        Returns:
            Dictionary mapping segment to average value
        """
        if 'segment' not in df.columns or 'monetary' not in df.columns:
            return {}

        avg_values = df.groupby('segment')['monetary'].agg(['mean', 'median', 'sum', 'count'])
        
        result = {}
        for segment in avg_values.index:
            result[segment] = {
                'avg_monetary': round(float(avg_values.loc[segment, 'mean']), 2),
                'median_monetary': round(float(avg_values.loc[segment, 'median']), 2),
                'total_revenue': round(float(avg_values.loc[segment, 'sum']), 2),
                'customer_count': int(avg_values.loc[segment, 'count'])
            }

        return result

    @staticmethod
    def identify_growth_opportunities(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Identify growth opportunity signals.

        Args:
            df: DataFrame with segment and RFM values

        Returns:
            Dictionary with growth opportunity signals
        """
        if 'segment' not in df.columns:
            return {}

        opportunities = {}

        # High Value segment growth opportunity
        high_value = df[df['segment'] == 'High Value']
        if not high_value.empty:
            opportunities['high_value_expansion'] = {
                'description': 'Expand High Value segment through targeted upselling',
                'current_size': len(high_value),
                'revenue_potential': round(high_value['monetary'].sum() * 0.15, 2) if 'monetary' in df.columns else 0,
                'action': 'Implement premium tier loyalty program'
            }

        # Loyal segment growth opportunity
        loyal = df[df['segment'] == 'Loyal']
        if not loyal.empty:
            opportunities['loyal_retention'] = {
                'description': 'Retain Loyal segment with enhanced engagement',
                'current_size': len(loyal),
                'revenue_at_risk': round(loyal['monetary'].sum() * 0.1, 2) if 'monetary' in df.columns else 0,
                'action': 'Increase engagement frequency through personalized communications'
            }

        # New segment conversion opportunity
        new = df[df['segment'] == 'New']
        if not new.empty:
            conversion_rate = 0.3  # Assume 30% conversion to repeat customer
            upside = round(len(new) * conversion_rate, 0)
            opportunities['new_customer_conversion'] = {
                'description': 'Convert New customers to repeat buyers',
                'current_size': len(new),
                'conversion_potential': int(upside),
                'estimated_additional_revenue': round(new['monetary'].mean() * upside, 2) if 'monetary' in df.columns else 0,
                'action': 'Deploy welcome series with exclusive first-purchase incentives'
            }

        # At Risk recovery opportunity
        at_risk = df[df['segment'] == 'At Risk']
        if not at_risk.empty:
            opportunities['at_risk_recovery'] = {
                'description': 'Win back At Risk customers before full churn',
                'current_size': len(at_risk),
                'recovery_rate': 0.3,  # Assume 30% recovery rate
                'estimated_recovered_revenue': round(at_risk['monetary'].mean() * len(at_risk) * 0.3, 2) if 'monetary' in df.columns else 0,
                'action': 'Launch "We Miss You" campaign with special comeback offers'
            }

        return opportunities

    @staticmethod
    def generate_segment_insights(db: Session, dataset_id: int) -> Dict[str, Any]:
        """
        Generate comprehensive insights from customer segments.

        Args:
            db: Database session
            dataset_id: ID of the dataset

        Returns:
            Dictionary with all insights
        """
        output_dir = settings.processed_data_path
        df = InsightService.load_segments_data(output_dir)

        if df is None or df.empty:
            return {
                'success': False,
                'message': 'No segment data found. Please run segmentation first.',
                'data': None
            }

        try:
            # Compute all insights
            top_revenue = InsightService.compute_top_revenue_segment(df)
            largest = InsightService.compute_largest_segment(df)
            at_risk = InsightService.compute_at_risk_volume(df)
            avg_values = InsightService.compute_avg_order_value_by_segment(df)
            opportunities = InsightService.identify_growth_opportunities(df)

            # Aggregate metrics
            segment_distribution = df['segment'].value_counts().to_dict()
            total_revenue = round(df['monetary'].sum(), 2) if 'monetary' in df.columns else 0
            avg_customer_value = round(df['monetary'].mean(), 2) if 'monetary' in df.columns else 0

            insights = {
                'timestamp': datetime.utcnow().isoformat(),
                'total_customers': len(df),
                'total_revenue': total_revenue,
                'avg_customer_value': avg_customer_value,
                'segment_distribution': {str(k): int(v) for k, v in segment_distribution.items()},
                'top_revenue_segment': top_revenue,
                'largest_segment': largest,
                'at_risk_metrics': at_risk,
                'segment_value_analysis': avg_values,
                'growth_opportunities': opportunities,
                'executive_summary': InsightService.generate_executive_summary(
                    top_revenue, largest, at_risk, opportunities, total_revenue
                )
            }

            return {
                'success': True,
                'message': 'Insights generated successfully',
                'data': insights
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Error generating insights: {str(e)}',
                'data': None
            }

    @staticmethod
    def generate_executive_summary(
        top_revenue: Dict,
        largest: Dict,
        at_risk: Dict,
        opportunities: Dict,
        total_revenue: float
    ) -> Dict[str, str]:
        """
        Generate executive summary of key insights.

        Args:
            top_revenue: Top revenue segment info
            largest: Largest segment info
            at_risk: At-risk metrics
            opportunities: Growth opportunities
            total_revenue: Total revenue

        Returns:
            Dictionary with summary statements
        """
        summary = {}

        if top_revenue:
            summary['revenue_focus'] = (
                f"{top_revenue.get('segment', 'N/A')} is the top revenue driver, "
                f"contributing {top_revenue.get('percentage_of_total', 0)}% of total revenue "
                f"({top_revenue.get('customer_count', 0)} customers)"
            )

        if largest:
            summary['volume_focus'] = (
                f"{largest.get('segment', 'N/A')} is the largest segment by count "
                f"({largest.get('customer_count', 0)} customers, {largest.get('percentage_of_total', 0)}% of base)"
            )

        if at_risk and at_risk.get('at_risk_count', 0) > 0:
            summary['risk_alert'] = (
                f"{at_risk.get('at_risk_count', 0)} customers are At Risk "
                f"({at_risk.get('percentage_of_total', 0)}% of base), representing "
                f"${at_risk.get('total_at_risk_revenue', 0)} in potential revenue loss"
            )

        if opportunities:
            opp_count = len(opportunities)
            summary['opportunity_count'] = f"{opp_count} growth opportunities identified"

        return summary
