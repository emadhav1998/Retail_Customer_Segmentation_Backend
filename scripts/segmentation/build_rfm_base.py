"""
Build RFM Base Script
Combines Recency, Frequency, and Monetary metrics into a unified RFM base table
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional


def load_data(file_path: str) -> pd.DataFrame:
    """Load data from CSV file."""
    return pd.read_csv(file_path, encoding='utf-8')


def finalize_recency(recency_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 1: Finalize and validate recency data.

    Args:
        recency_df: DataFrame with customerid and recency columns

    Returns:
        Tuple of (recency_df, stats_dict)
    """
    if 'customerid' not in recency_df.columns or 'recency' not in recency_df.columns:
        return pd.DataFrame(), {'error': 'Missing required columns: customerid, recency'}

    recency_df = recency_df.copy()

    # Ensure recency is numeric
    recency_df['recency'] = pd.to_numeric(recency_df['recency'], errors='coerce')

    # Remove any rows with null recency
    recency_df = recency_df.dropna(subset=['recency'])

    stats = {
        'unique_customers': len(recency_df),
        'recency_min': int(recency_df['recency'].min()),
        'recency_max': int(recency_df['recency'].max()),
        'recency_mean': round(float(recency_df['recency'].mean()), 2),
    }

    return recency_df, stats


def validate_frequency(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 2: Calculate and validate frequency metrics.

    Frequency is the number of unique invoices per customer.

    Args:
        df: Input DataFrame with customerid and invoiceno columns

    Returns:
        Tuple of (frequency_df, stats_dict)
    """
    if 'customerid' not in df.columns or 'invoiceno' not in df.columns:
        return pd.DataFrame(), {'error': 'Missing required columns: customerid, invoiceno'}

    customer_frequency = (
        df.groupby('customerid')['invoiceno']
        .nunique()
        .reset_index()
    )
    customer_frequency.columns = ['customerid', 'frequency']

    # Ensure frequency is positive
    customer_frequency = customer_frequency[customer_frequency['frequency'] > 0]

    stats = {
        'unique_customers': len(customer_frequency),
        'frequency_min': int(customer_frequency['frequency'].min()),
        'frequency_max': int(customer_frequency['frequency'].max()),
        'frequency_mean': round(float(customer_frequency['frequency'].mean()), 2),
    }

    return customer_frequency, stats


def calculate_monetary(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 3: Calculate monetary value (total revenue) per customer.

    Args:
        df: Input DataFrame with customerid, quantity, unitprice columns

    Returns:
        Tuple of (monetary_df, stats_dict)
    """
    if 'customerid' not in df.columns:
        return pd.DataFrame(), {'error': 'Missing required column: customerid'}

    # Use pre-computed total_revenue if available, otherwise compute it
    df = df.copy()
    if 'total_revenue' not in df.columns:
        if 'quantity' in df.columns and 'unitprice' in df.columns:
            df['total_revenue'] = df['quantity'] * df['unitprice']
        else:
            return pd.DataFrame(), {'error': 'Missing required columns: quantity, unitprice'}

    customer_monetary = (
        df.groupby('customerid')['total_revenue']
        .sum()
        .reset_index()
    )
    customer_monetary.columns = ['customerid', 'monetary']
    customer_monetary['monetary'] = customer_monetary['monetary'].round(2)

    # Only keep positive monetary values
    customer_monetary = customer_monetary[customer_monetary['monetary'] > 0]

    stats = {
        'unique_customers': len(customer_monetary),
        'monetary_min': round(float(customer_monetary['monetary'].min()), 2),
        'monetary_max': round(float(customer_monetary['monetary'].max()), 2),
        'monetary_mean': round(float(customer_monetary['monetary'].mean()), 2),
    }

    return customer_monetary, stats


def merge_rfm_components(
    recency_df: pd.DataFrame,
    frequency_df: pd.DataFrame,
    monetary_df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 4: Merge recency, frequency, and monetary into a unified RFM base.

    Args:
        recency_df: Customer-level recency data
        frequency_df: Customer-level frequency data
        monetary_df: Customer-level monetary data

    Returns:
        Tuple of (rfm_base, merge_stats)
    """
    # Start with recency as the base
    rfm = recency_df.merge(frequency_df, on='customerid', how='inner')
    rfm = rfm.merge(monetary_df, on='customerid', how='inner')

    merge_stats = {
        'unique_customers': len(rfm),
        'columns': list(rfm.columns),
    }

    return rfm, merge_stats


def validate_rfm_base(rfm: pd.DataFrame) -> Tuple[bool, Optional[str]]:
    """Validate that the RFM base has all required columns and valid data."""
    required = ['customerid', 'recency', 'frequency', 'monetary']
    missing = [c for c in required if c not in rfm.columns]
    if missing:
        return False, f"Missing columns: {', '.join(missing)}"

    # Check for any nulls in required columns
    if rfm[required].isnull().any().any():
        return False, "Found null values in required columns"

    return True, None


def generate_column_statistics(rfm: pd.DataFrame) -> Dict[str, Any]:
    """Generate comprehensive column statistics for the RFM base."""
    stats = {
        'total_customers': len(rfm),
        'columns': list(rfm.columns),
        'column_count': len(rfm.columns),
        'recency': {
            'min': int(rfm['recency'].min()),
            'max': int(rfm['recency'].max()),
            'mean': round(float(rfm['recency'].mean()), 2),
            'median': int(rfm['recency'].median()),
            'std': round(float(rfm['recency'].std()), 2),
        },
        'frequency': {
            'min': int(rfm['frequency'].min()),
            'max': int(rfm['frequency'].max()),
            'mean': round(float(rfm['frequency'].mean()), 2),
            'median': int(rfm['frequency'].median()),
            'std': round(float(rfm['frequency'].std()), 2),
        },
        'monetary': {
            'min': round(float(rfm['monetary'].min()), 2),
            'max': round(float(rfm['monetary'].max()), 2),
            'mean': round(float(rfm['monetary'].mean()), 2),
            'median': round(float(rfm['monetary'].median()), 2),
            'std': round(float(rfm['monetary'].std()), 2),
        },
    }
    return stats


def run_build_rfm_base(
    cleaned_file: str,
    recency_file: str,
    output_dir: str,
    save_summary: bool = True
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run complete RFM base building pipeline.

    Args:
        cleaned_file: Path to cleaned_final.csv
        recency_file: Path to rfm_recency.csv
        output_dir: Directory to save output files
        save_summary: Whether to save summary JSON

    Returns:
        Tuple of (success, message, summary_dict)
    """
    try:
        # Load input files
        if not os.path.exists(cleaned_file):
            return False, f"Cleaned file not found: {cleaned_file}", {}
        if not os.path.exists(recency_file):
            return False, f"Recency file not found: {recency_file}", {}

        cleaned_df = load_data(cleaned_file)
        recency_df = load_data(recency_file)

        # Step 1: Finalize recency
        recency_df, recency_stats = finalize_recency(recency_df)
        if recency_df.empty:
            return False, "Failed to finalize recency data", {}

        # Step 2: Validate frequency
        frequency_df, frequency_stats = validate_frequency(cleaned_df)
        if frequency_df.empty:
            return False, "Failed to calculate frequency", {}

        # Step 3: Calculate monetary (renamed from total revenue)
        monetary_df, monetary_stats = calculate_monetary(cleaned_df)
        if monetary_df.empty:
            return False, "Failed to calculate monetary value", {}

        # Step 4: Merge components
        rfm, merge_stats = merge_rfm_components(recency_df, frequency_df, monetary_df)

        # Validate the merged RFM base
        is_valid, error_msg = validate_rfm_base(rfm)
        if not is_valid:
            return False, error_msg, {}

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Save RFM base
        output_file = os.path.join(output_dir, 'rfm_base.csv')
        rfm.to_csv(output_file, index=False)

        # Generate column statistics
        column_stats = generate_column_statistics(rfm)

        summary = {
            'step': 'build_rfm_base',
            'timestamp': datetime.now().isoformat(),
            'input_files': {
                'cleaned': cleaned_file,
                'recency': recency_file,
            },
            'output_file': output_file,
            'recency_stats': recency_stats,
            'frequency_stats': frequency_stats,
            'monetary_stats': monetary_stats,
            'merge_stats': merge_stats,
            'column_statistics': column_stats,
        }

        if save_summary:
            summary_file = os.path.join(output_dir, 'rfm_base_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)

        return True, f"RFM base built successfully. Output saved to {output_file}", summary

    except Exception as e:
        return False, f"Error building RFM base: {str(e)}", {}


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Usage: python build_rfm_base.py <cleaned_file> <recency_file> <output_dir>")
        sys.exit(1)

    cleaned_file = sys.argv[1]
    recency_file = sys.argv[2]
    output_dir = sys.argv[3]

    success, message, summary = run_build_rfm_base(cleaned_file, recency_file, output_dir)

    if success:
        print(f"Success: {message}")
        print(json.dumps(summary, indent=2))
    else:
        print(f"Error: {message}")
        sys.exit(1)