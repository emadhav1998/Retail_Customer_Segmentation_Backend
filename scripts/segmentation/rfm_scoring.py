"""
RFM Scoring Script
Assigns R, F, M scores (1–5) to each customer using quintile binning
and computes a combined RFM score
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional


def load_rfm_base(file_path: str) -> pd.DataFrame:
    """Load the RFM base file."""
    return pd.read_csv(file_path, encoding='utf-8')


def score_recency(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Step 1: Score recency on a 1–5 scale.

    Lower recency (more recent) gets a higher score.
    Customers are divided into 5 equal-sized groups (quintiles).

    Args:
        rfm: DataFrame with recency column

    Returns:
        DataFrame with recency_score column added
    """
    # Use rank-based percentile to avoid duplicate-edge issues
    rfm = rfm.copy()
    rfm['recency_score'] = pd.qcut(
        rfm['recency'].rank(method='first'),
        q=5,
        labels=[5, 4, 3, 2, 1]   # Reverse: lower recency (recent) = score 5
    ).astype(int)
    return rfm


def score_frequency(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Step 2: Score frequency on a 1–5 scale.

    Higher frequency gets a higher score.

    Args:
        rfm: DataFrame with frequency column

    Returns:
        DataFrame with frequency_score column added
    """
    rfm = rfm.copy()
    rfm['frequency_score'] = pd.qcut(
        rfm['frequency'].rank(method='first'),
        q=5,
        labels=[1, 2, 3, 4, 5]   # Higher frequency = higher score
    ).astype(int)
    return rfm


def score_monetary(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Step 3: Score monetary value on a 1–5 scale.

    Higher monetary value gets a higher score.

    Args:
        rfm: DataFrame with monetary column

    Returns:
        DataFrame with monetary_score column added
    """
    rfm = rfm.copy()
    rfm['monetary_score'] = pd.qcut(
        rfm['monetary'].rank(method='first'),
        q=5,
        labels=[1, 2, 3, 4, 5]   # Higher monetary = higher score
    ).astype(int)
    return rfm


def calculate_combined_rfm_score(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Step 4: Compute a combined RFM score.

    Combined score = concatenation of R, F, M score digits (e.g., "555")
    Also computes a numeric rfm_total as weighted sum: R*100 + F*10 + M.

    Args:
        rfm: DataFrame with recency_score, frequency_score, monetary_score

    Returns:
        DataFrame with rfm_score and rfm_total columns added
    """
    rfm = rfm.copy()
    rfm['rfm_score'] = (
        rfm['recency_score'].astype(str)
        + rfm['frequency_score'].astype(str)
        + rfm['monetary_score'].astype(str)
    )
    rfm['rfm_total'] = (
        rfm['recency_score'] * 100
        + rfm['frequency_score'] * 10
        + rfm['monetary_score']
    )
    return rfm


def generate_score_summary(rfm: pd.DataFrame) -> Dict[str, Any]:
    """Generate summary statistics for the scored RFM data."""
    score_distribution = rfm['rfm_score'].value_counts().head(20).to_dict()

    return {
        'unique_customers': len(rfm),
        'recency_score_distribution': rfm['recency_score'].value_counts().sort_index().to_dict(),
        'frequency_score_distribution': rfm['frequency_score'].value_counts().sort_index().to_dict(),
        'monetary_score_distribution': rfm['monetary_score'].value_counts().sort_index().to_dict(),
        'rfm_total_stats': {
            'min': int(rfm['rfm_total'].min()),
            'max': int(rfm['rfm_total'].max()),
            'mean': round(float(rfm['rfm_total'].mean()), 2),
        },
        'top_rfm_scores': score_distribution,
    }


def run_rfm_scoring(
    input_file: str,
    output_dir: str,
    save_summary: bool = True
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run complete RFM scoring pipeline.

    Args:
        input_file: Path to rfm_base.csv
        output_dir: Directory to save output files
        save_summary: Whether to save summary JSON

    Returns:
        Tuple of (success, message, summary_dict)
    """
    try:
        rfm = load_rfm_base(input_file)

        # Validate required columns
        required = ['customerid', 'recency', 'frequency', 'monetary']
        missing = [c for c in required if c not in rfm.columns]
        if missing:
            return False, f"Missing required columns: {', '.join(missing)}", {}

        # Score each dimension
        rfm = score_recency(rfm)
        rfm = score_frequency(rfm)
        rfm = score_monetary(rfm)

        # Combine into overall RFM score
        rfm = calculate_combined_rfm_score(rfm)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Save scored output
        output_file = os.path.join(output_dir, 'rfm_scores.csv')
        rfm.to_csv(output_file, index=False)

        summary = {
            'step': 'rfm_scoring',
            'timestamp': datetime.now().isoformat(),
            'input_file': input_file,
            'output_file': output_file,
            'score_summary': generate_score_summary(rfm),
            'columns': list(rfm.columns),
            'column_count': len(rfm.columns),
        }

        if save_summary:
            summary_file = os.path.join(output_dir, 'rfm_scoring_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)

        return True, f"RFM scoring completed. Output saved to {output_file}", summary

    except Exception as e:
        return False, f"Error during RFM scoring: {str(e)}", {}


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python rfm_scoring.py <input_file> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    success, message, summary = run_rfm_scoring(input_file, output_dir)

    if success:
        print(f"Success: {message}")
        print(json.dumps(summary, indent=2))
    else:
        print(f"Error: {message}")
        sys.exit(1)