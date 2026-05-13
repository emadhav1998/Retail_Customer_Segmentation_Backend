"""
Segment Mapping Script
Maps RFM scores to business-meaningful customer segments
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional


def load_rfm_scores(file_path: str) -> pd.DataFrame:
    """Load the RFM scores file."""
    return pd.read_csv(file_path, encoding='utf-8')


def assign_segments(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Step 1: Assign customer segments based on RFM scores.

    Segmentation rules:
    - High Value: R>=4, F>=4, M>=4 (best customers)
    - Loyal: R>=3, F>=4, M>=3 (repeat customers with good spend)
    - New: R>=4, F<=2, M>=2 (recent but not yet frequent)
    - At Risk: R<=2, F>=3, M>=2 (used to spend, haven't recently)
    - Low Engagement: R<=2, F<=2, M<=2 (inactive/minimal spend)

    Args:
        rfm: DataFrame with recency_score, frequency_score, monetary_score

    Returns:
        DataFrame with segment column added
    """
    rfm = rfm.copy()

    # Default segment
    rfm['segment'] = 'Other'

    # High Value: Best customers on all dimensions
    mask = (rfm['recency_score'] >= 4) & (rfm['frequency_score'] >= 4) & (rfm['monetary_score'] >= 4)
    rfm.loc[mask, 'segment'] = 'High Value'

    # Loyal: Good recency and frequency with decent spend
    mask = (rfm['recency_score'] >= 3) & (rfm['frequency_score'] >= 4) & (rfm['monetary_score'] >= 3)
    rfm.loc[mask, 'segment'] = 'Loyal'

    # New: Recent and decent spend but low frequency (new customers)
    mask = (rfm['recency_score'] >= 4) & (rfm['frequency_score'] <= 2) & (rfm['monetary_score'] >= 2)
    rfm.loc[mask, 'segment'] = 'New'

    # At Risk: Poor recency but good past frequency/spend (churning)
    mask = (rfm['recency_score'] <= 2) & (rfm['frequency_score'] >= 3) & (rfm['monetary_score'] >= 2)
    rfm.loc[mask, 'segment'] = 'At Risk'

    # Low Engagement: Poor on all dimensions (dormant/inactive)
    mask = (rfm['recency_score'] <= 2) & (rfm['frequency_score'] <= 2) & (rfm['monetary_score'] <= 2)
    rfm.loc[mask, 'segment'] = 'Low Engagement'

    return rfm


def calculate_segment_metrics(rfm: pd.DataFrame) -> Dict[str, Any]:
    """
    Step 2: Calculate key metrics for each segment.

    Args:
        rfm: DataFrame with segment column

    Returns:
        Dictionary with segment metrics
    """
    metrics = {}

    for segment in rfm['segment'].unique():
        segment_data = rfm[rfm['segment'] == segment]

        metrics[segment] = {
            'customer_count': len(segment_data),
            'percentage_of_total': round((len(segment_data) / len(rfm) * 100), 2),
            'avg_recency': round(segment_data['recency'].mean(), 2),
            'avg_frequency': round(segment_data['frequency'].mean(), 2),
            'avg_monetary': round(segment_data['monetary'].mean(), 2),
            'total_revenue': round(segment_data['monetary'].sum(), 2),
            'revenue_percentage': round((segment_data['monetary'].sum() / rfm['monetary'].sum() * 100), 2),
        }

    return metrics


def rank_segments_by_importance(metrics: Dict[str, Any]) -> Dict[int, str]:
    """
    Step 3: Rank segments by revenue contribution (importance).

    Args:
        metrics: Segment metrics dictionary

    Returns:
        Dictionary mapping rank to segment name
    """
    # Sort segments by revenue contribution
    sorted_segments = sorted(
        metrics.items(),
        key=lambda x: x[1]['total_revenue'],
        reverse=True
    )

    ranking = {i + 1: segment[0] for i, segment in enumerate(sorted_segments)}

    return ranking


def generate_segment_summary(rfm: pd.DataFrame, metrics: Dict[str, Any], ranking: Dict[int, str]) -> Dict[str, Any]:
    """Generate comprehensive segment summary."""
    return {
        'total_customers': len(rfm),
        'total_revenue': round(rfm['monetary'].sum(), 2),
        'unique_segments': rfm['segment'].nunique(),
        'segment_counts': {segment: count for segment, count in rfm['segment'].value_counts().to_dict().items()},
        'segment_metrics': metrics,
        'segment_ranking': ranking,
    }


def run_segment_mapping(
    rfm_scores_file: str,
    output_dir: str,
    save_summary: bool = True
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run complete segment mapping pipeline.

    Args:
        rfm_scores_file: Path to rfm_scores.csv
        output_dir: Directory to save output files
        save_summary: Whether to save summary JSON

    Returns:
        Tuple of (success, message, summary_dict)
    """
    try:
        # Load RFM scores
        if not os.path.exists(rfm_scores_file):
            return False, f"RFM scores file not found: {rfm_scores_file}", {}

        rfm = load_rfm_scores(rfm_scores_file)

        # Validate required columns
        required = ['customerid', 'recency_score', 'frequency_score', 'monetary_score', 'monetary']
        missing = [c for c in required if c not in rfm.columns]
        if missing:
            return False, f"Missing required columns: {', '.join(missing)}", {}

        # Step 1: Assign segments
        rfm = assign_segments(rfm)

        # Step 2: Calculate segment metrics
        metrics = calculate_segment_metrics(rfm)

        # Step 3: Rank segments by importance
        ranking = rank_segments_by_importance(metrics)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Save customer segments
        output_file = os.path.join(output_dir, 'customer_segments.csv')
        rfm.to_csv(output_file, index=False)

        # Generate summary
        summary = {
            'step': 'segment_mapping',
            'timestamp': datetime.now().isoformat(),
            'input_file': rfm_scores_file,
            'output_file': output_file,
            'segment_summary': generate_segment_summary(rfm, metrics, ranking),
            'columns': list(rfm.columns),
            'column_count': len(rfm.columns),
        }

        if save_summary:
            summary_file = os.path.join(output_dir, 'segment_mapping_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)

        return True, f"Segment mapping completed. Output saved to {output_file}", summary

    except Exception as e:
        return False, f"Error during segment mapping: {str(e)}", {}


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python segment_mapping.py <rfm_scores_file> <output_dir>")
        sys.exit(1)

    rfm_scores_file = sys.argv[1]
    output_dir = sys.argv[2]

    success, message, summary = run_segment_mapping(rfm_scores_file, output_dir)

    if success:
        print(f"Success: {message}")
        print(json.dumps(summary, indent=2))
    else:
        print(f"Error: {message}")
        sys.exit(1)