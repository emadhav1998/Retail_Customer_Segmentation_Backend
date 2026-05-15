"""
AI Summary Service
Generates structured executive summaries from customer segment metrics
"""
import os
import json
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.insight_service import InsightService


# Segment playbook: description, headline, action, color
SEGMENT_PLAYBOOK: Dict[str, Dict[str, str]] = {
    'High Value': {
        'headline': 'Top revenue drivers — protect and grow',
        'description': (
            'High Value customers deliver the highest revenue and purchase most frequently. '
            'They represent the core of business profitability and must be retained at all costs.'
        ),
        'action': 'Launch a VIP loyalty programme with exclusive benefits and priority support.',
        'urgency': 'Maintain'
    },
    'Loyal': {
        'headline': 'Consistent buyers — deepen the relationship',
        'description': (
            'Loyal customers return regularly and provide predictable revenue. '
            'They are strong candidates for upselling and cross-selling initiatives.'
        ),
        'action': 'Introduce tiered rewards and personalised product recommendations.',
        'urgency': 'Grow'
    },
    'New': {
        'headline': 'Recent acquistions — convert to repeat buyers',
        'description': (
            'New customers have made an initial purchase but have not yet established a '
            'purchasing habit. Early engagement is critical to long-term retention.'
        ),
        'action': 'Deploy a welcome series with a second-purchase incentive within 14 days.',
        'urgency': 'Activate'
    },
    'At Risk': {
        'headline': 'Churning customers — win back before they leave',
        'description': (
            'At Risk customers were once active but have gone quiet. '
            'They represent a significant revenue recovery opportunity with targeted campaigns.'
        ),
        'action': 'Launch a "We Miss You" win-back campaign with a limited-time offer.',
        'urgency': 'Urgent'
    },
    'Low Engagement': {
        'headline': 'Dormant segment — cost-effective reactivation',
        'description': (
            'Low Engagement customers show minimal activity and spend. '
            'A small percentage can be reactivated with the right message at low cost.'
        ),
        'action': 'Run a low-cost A/B test to identify reactivation triggers.',
        'urgency': 'Monitor'
    },
}


class AISummaryService:
    """Service for generating AI-style executive summaries from segment data"""

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
    def build_segment_metrics(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Compute per-segment metrics from the DataFrame.

        Args:
            df: Customer segments DataFrame

        Returns:
            List of dicts with per-segment metrics
        """
        metrics = []
        total_revenue = df['monetary'].sum() if 'monetary' in df.columns else 1

        for segment, group in df.groupby('segment'):
            revenue = group['monetary'].sum() if 'monetary' in df.columns else 0
            metrics.append({
                'segment': str(segment),
                'customer_count': len(group),
                'total_revenue': round(float(revenue), 2),
                'avg_revenue': round(float(group['monetary'].mean()), 2) if 'monetary' in df.columns else 0,
                'percentage_of_customers': round(len(group) / len(df) * 100, 1),
                'percentage_of_revenue': round(revenue / total_revenue * 100, 1) if total_revenue > 0 else 0,
            })

        # Sort by total revenue descending
        metrics.sort(key=lambda x: x['total_revenue'], reverse=True)
        return metrics

    @staticmethod
    def generate_executive_text(
        segment_metrics: List[Dict[str, Any]],
        total_customers: int,
        total_revenue: float,
        include_risk: bool = True,
        include_growth: bool = True
    ) -> Tuple[str, List[str]]:
        """
        Generate executive summary text and bullet points from segment metrics.

        Args:
            segment_metrics: List of per-segment metric dicts
            total_customers: Total customer count
            total_revenue: Total revenue
            include_risk: Include at-risk analysis in text
            include_growth: Include growth signals in text

        Returns:
            Tuple of (full summary paragraph, list of bullet points)
        """
        # Identify top segments
        top_revenue_seg = segment_metrics[0]['segment'] if segment_metrics else 'N/A'
        top_revenue_pct = segment_metrics[0].get('percentage_of_revenue', 0) if segment_metrics else 0

        sorted_by_count = sorted(segment_metrics, key=lambda x: x['customer_count'], reverse=True)
        largest_seg = sorted_by_count[0]['segment'] if sorted_by_count else 'N/A'
        largest_count = sorted_by_count[0]['customer_count'] if sorted_by_count else 0

        at_risk = next((m for m in segment_metrics if m['segment'] == 'At Risk'), None)
        high_value = next((m for m in segment_metrics if m['segment'] == 'High Value'), None)
        new_seg = next((m for m in segment_metrics if m['segment'] == 'New'), None)

        # Build full paragraph
        lines = []
        lines.append(
            f"Analysis of {total_customers:,} customers generating ${total_revenue:,.0f} in total revenue "
            f"reveals five distinct behavioural segments with clear strategic implications."
        )

        if high_value:
            lines.append(
                f"The High Value segment ({high_value['customer_count']:,} customers) "
                f"is the primary revenue engine, contributing {high_value['percentage_of_revenue']:.1f}% "
                f"of total revenue despite representing only {high_value['percentage_of_customers']:.1f}% "
                f"of the customer base — a clear sign of revenue concentration risk that warrants "
                f"a dedicated retention programme."
            )

        if include_risk and at_risk:
            lines.append(
                f"A significant at-risk cohort of {at_risk['customer_count']:,} customers "
                f"(${at_risk['total_revenue']:,.0f} in historical revenue) is showing signs of churn. "
                f"With an estimated 30% recovery rate, a targeted win-back campaign could recover "
                f"approximately ${at_risk['total_revenue'] * 0.3:,.0f} in revenue."
            )

        if include_growth and new_seg:
            lines.append(
                f"The New segment ({new_seg['customer_count']:,} customers) presents the strongest "
                f"organic growth opportunity. Converting 30–40% to repeat buyers through a structured "
                f"onboarding sequence could add meaningful incremental revenue over the next 90 days."
            )

        lines.append(
            f"Immediate priorities are: (1) protect High Value and Loyal customers through premium "
            f"engagement, (2) recover At Risk customers before full churn, and (3) activate New "
            f"customers with a targeted first-repeat-purchase incentive."
        )

        full_text = ' '.join(lines)

        # Build bullet points
        bullets = [
            f"{top_revenue_seg} segment drives {top_revenue_pct:.1f}% of total revenue",
            f"{largest_seg} is the largest segment with {largest_count:,} customers",
        ]
        if at_risk:
            bullets.append(
                f"{at_risk['customer_count']:,} At Risk customers represent "
                f"${at_risk['total_revenue']:,.0f} in recoverable revenue"
            )
        if new_seg:
            bullets.append(
                f"{new_seg['customer_count']:,} New customers are ready for conversion campaigns"
            )
        bullets.append("5 strategic actions identified across all segments")

        return full_text, bullets

    @staticmethod
    def generate_segment_summaries(
        segment_metrics: List[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        """Generate per-segment headline/detail/action lines."""
        summaries = []
        for metric in segment_metrics:
            seg = metric['segment']
            playbook = SEGMENT_PLAYBOOK.get(seg, {
                'headline': f'{seg} segment',
                'description': f'{seg} customers require tailored engagement.',
                'action': 'Review segment behaviour and define targeted strategy.',
                'urgency': 'Review'
            })
            summaries.append({
                'segment': seg,
                'headline': playbook['headline'],
                'detail': (
                    f"{metric['customer_count']:,} customers "
                    f"({metric['percentage_of_customers']:.1f}% of base), "
                    f"${metric['total_revenue']:,.0f} revenue "
                    f"({metric['percentage_of_revenue']:.1f}% of total). "
                    + playbook['description']
                ),
                'action': playbook['action'],
                'urgency': playbook['urgency']
            })
        return summaries

    @staticmethod
    def generate_risk_summary(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Generate risk summary from at-risk segment data."""
        if 'segment' not in df.columns:
            return None

        at_risk_df = df[df['segment'] == 'At Risk']
        if at_risk_df.empty:
            return {
                'at_risk_count': 0,
                'at_risk_revenue': 0.0,
                'recovery_potential': 0.0,
                'urgency': 'Low',
                'recommended_action': 'No at-risk customers detected — maintain monitoring cadence.'
            }

        at_risk_revenue = float(at_risk_df['monetary'].sum()) if 'monetary' in df.columns else 0
        pct_of_base = len(at_risk_df) / len(df) * 100

        urgency = 'High' if pct_of_base > 15 else ('Medium' if pct_of_base > 8 else 'Low')

        return {
            'at_risk_count': int(len(at_risk_df)),
            'at_risk_revenue': round(at_risk_revenue, 2),
            'recovery_potential': round(at_risk_revenue * 0.3, 2),
            'urgency': urgency,
            'recommended_action': 'Launch a "We Miss You" campaign with a 10–15% discount code, '
                                   'targeting customers inactive for 60+ days.'
        }

    @staticmethod
    def generate_growth_signals(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate growth opportunity signals."""
        signals = []
        total_revenue = float(df['monetary'].sum()) if 'monetary' in df.columns else 0

        for seg, config in [
            ('High Value', {'uplift_factor': 0.15, 'action': 'Introduce premium upsell tier with exclusive access'}),
            ('Loyal', {'uplift_factor': 0.10, 'action': 'Cross-sell adjacent product categories'}),
            ('New', {'uplift_factor': 0.30, 'action': 'Deploy automated first-repeat-purchase email sequence'}),
            ('At Risk', {'uplift_factor': 0.30, 'action': 'Win-back campaign with time-limited special offer'}),
        ]:
            seg_df = df[df['segment'] == seg] if 'segment' in df.columns else pd.DataFrame()
            if seg_df.empty:
                continue
            seg_revenue = float(seg_df['monetary'].sum()) if 'monetary' in df.columns else 0
            uplift = round(seg_revenue * config['uplift_factor'], 2)
            signals.append({
                'opportunity': f"Grow {seg} segment contribution",
                'segment': seg,
                'estimated_uplift': uplift,
                'action': config['action']
            })

        return signals

    @staticmethod
    def build_recommendations(
        segment_metrics: List[Dict[str, Any]],
        include_risk: bool = True,
        include_growth: bool = True
    ) -> List[str]:
        """Build prioritised recommendation list."""
        recs = [
            "1. Immediately protect High Value customers with a VIP retention programme.",
            "2. Launch At Risk win-back campaign (email + offer) within the next 7 days.",
            "3. Deploy a New customer onboarding sequence targeting a second purchase.",
            "4. Introduce Loyal customer upsell/cross-sell bundles to increase order value.",
            "5. Run a low-cost reactivation test for Low Engagement segment.",
            "6. Set up monthly RFM re-scoring to catch segment migrations early.",
            "7. Define churn early-warning triggers (e.g., 45-day order gap alert).",
        ]
        return recs

    @staticmethod
    def generate_summary(
        db: Session,
        dataset_id: int,
        include_recommendations: bool = True,
        include_risk_analysis: bool = True,
        include_growth_signals: bool = True,
        external_metrics: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Main summary generation workflow.

        Args:
            db: Database session
            dataset_id: ID of the dataset
            include_recommendations: Include prioritised recommendations
            include_risk_analysis: Include risk summary section
            include_growth_signals: Include growth signal section
            external_metrics: Optional pre-computed segment metrics list

        Returns:
            Dict with success flag, message, and summary data
        """
        output_dir = settings.processed_data_path
        df = AISummaryService.load_segments_data(output_dir)

        if df is None or df.empty:
            return {
                'success': False,
                'message': 'No segment data found. Please run segmentation first.',
                'data': None
            }

        try:
            total_customers = len(df)
            total_revenue = round(float(df['monetary'].sum()), 2) if 'monetary' in df.columns else 0

            # Use external metrics if provided, otherwise compute from DataFrame
            segment_metrics = external_metrics or AISummaryService.build_segment_metrics(df)

            # Generate all components
            executive_text, bullets = AISummaryService.generate_executive_text(
                segment_metrics,
                total_customers,
                total_revenue,
                include_risk=include_risk_analysis,
                include_growth=include_growth_signals
            )
            segment_summaries = AISummaryService.generate_segment_summaries(segment_metrics)
            risk_summary = AISummaryService.generate_risk_summary(df) if include_risk_analysis else None
            growth_signals = AISummaryService.generate_growth_signals(df) if include_growth_signals else None
            recommendations = AISummaryService.build_recommendations(segment_metrics) if include_recommendations else None

            summary_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'dataset_id': dataset_id,
                'total_customers': total_customers,
                'total_revenue': total_revenue,
                'executive_summary': executive_text,
                'executive_bullets': bullets,
                'segment_summaries': segment_summaries,
                'risk_summary': risk_summary,
                'growth_signals': growth_signals,
                'recommendations': recommendations
            }

            # Persist latest summary to disk for fast retrieval
            summary_path = os.path.join(output_dir, 'ai_summary.json')
            with open(summary_path, 'w') as f:
                json.dump(summary_data, f, indent=2)

            return {
                'success': True,
                'message': 'AI summary generated successfully',
                'data': summary_data
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Error generating AI summary: {str(e)}',
                'data': None
            }

    @staticmethod
    def get_latest_summary(dataset_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest persisted AI summary from disk.

        Args:
            dataset_id: ID of the dataset

        Returns:
            Summary dict or None if not found
        """
        try:
            summary_path = os.path.join(settings.processed_data_path, 'ai_summary.json')
            if not os.path.exists(summary_path):
                return None
            with open(summary_path, 'r') as f:
                return json.load(f)
        except Exception:
            return None
