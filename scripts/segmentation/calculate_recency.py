"""
Recency Calculation Script
Calculates recency metrics for RFM analysis
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load data from CSV file.
    
    Args:
        file_path: Path to the data file
        
    Returns:
        DataFrame with loaded data
    """
    df = pd.read_csv(file_path, encoding='utf-8')
    return df


def set_reference_date(df: pd.DataFrame, reference_date: Optional[str] = None) -> Tuple[datetime, Dict[str, Any]]:
    """
    Step 1: Set reference date for recency calculation.
    
    The reference date is typically the maximum date in the dataset + 1 day,
    or a user-specified date.
    
    Args:
        df: Input DataFrame with invoicedate column
        reference_date: Optional reference date as string (YYYY-MM-DD)
        
    Returns:
        Tuple of (reference_date, stats_dict)
    """
    # Ensure invoicedate is datetime
    if 'invoicedate' in df.columns:
        df['invoicedate'] = pd.to_datetime(df['invoicedate'], errors='coerce')
    
    # If reference date not specified, use max date + 1 day
    if reference_date is None:
        max_date = df['invoicedate'].max()
        reference_date_obj = max_date + timedelta(days=1)
    else:
        reference_date_obj = pd.Timestamp(reference_date)
    
    stats = {
        'reference_date': reference_date_obj.isoformat(),
        'max_date_in_data': df['invoicedate'].max().isoformat() if 'invoicedate' in df.columns else None,
        'min_date_in_data': df['invoicedate'].min().isoformat() if 'invoicedate' in df.columns else None,
        'date_range_days': (df['invoicedate'].max() - df['invoicedate'].min()).days if 'invoicedate' in df.columns else 0
    }
    
    return reference_date_obj, stats


def calculate_recency(df: pd.DataFrame, reference_date: datetime) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 2: Calculate recency for each customer.
    
    Recency is the number of days since the last purchase.
    
    Args:
        df: Input DataFrame with invoicedate and customerid columns
        reference_date: Reference date for calculation
        
    Returns:
        Tuple of (recency_df, stats_dict)
    """
    # Ensure required columns exist
    if 'customerid' not in df.columns or 'invoicedate' not in df.columns:
        return df, {'error': 'Missing required columns: customerid, invoicedate'}
    
    # Ensure invoicedate is datetime
    df['invoicedate'] = pd.to_datetime(df['invoicedate'], errors='coerce')
    
    # Calculate last purchase date for each customer
    customer_last_purchase = df.groupby('customerid')['invoicedate'].max().reset_index()
    customer_last_purchase.columns = ['customerid', 'last_purchase_date']
    
    # Calculate recency (days since last purchase)
    customer_last_purchase['recency'] = (reference_date - customer_last_purchase['last_purchase_date']).dt.days
    
    # Merge back to original dataframe
    df = df.merge(customer_last_purchase[['customerid', 'recency']], on='customerid', how='left')
    
    # Calculate statistics
    stats = {
        'unique_customers': customer_last_purchase.shape[0],
        'recency_min': customer_last_purchase['recency'].min(),
        'recency_max': customer_last_purchase['recency'].max(),
        'recency_mean': round(customer_last_purchase['recency'].mean(), 2),
        'recency_median': customer_last_purchase['recency'].median(),
        'recency_std': round(customer_last_purchase['recency'].std(), 2)
    }
    
    return df, stats


def add_recency_to_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 3: Add recency information to the dataset.
    
    This creates a customer-level recency summary.
    
    Args:
        df: Input DataFrame with recency column
        
    Returns:
        DataFrame with recency added
    """
    # The recency column has already been added in calculate_recency
    # This function serves as a validation step
    
    if 'recency' not in df.columns:
        raise ValueError("Recency column not found in DataFrame")
    
    return df


def calculate_recency_quartiles(customer_recency: pd.DataFrame) -> Dict[str, float]:
    """
    Step 4: Calculate recency quartiles for later scoring.
    
    Args:
        customer_recency: DataFrame with unique customers and their recency
        
    Returns:
        Dictionary with quartile values
    """
    quartiles = {
        'q1': customer_recency['recency'].quantile(0.25),
        'q2': customer_recency['recency'].quantile(0.50),
        'q3': customer_recency['recency'].quantile(0.75),
        'q4': customer_recency['recency'].quantile(1.00)
    }
    
    return quartiles


def generate_recency_summary(df: pd.DataFrame, customer_recency: pd.DataFrame, quartiles: Dict) -> Dict[str, Any]:
    """
    Generate summary statistics for recency.
    
    Args:
        df: Full dataset with recency
        customer_recency: Customer-level recency data
        quartiles: Recency quartiles
        
    Returns:
        Dictionary with summary statistics
    """
    summary = {
        'total_transactions': len(df),
        'unique_customers': len(customer_recency),
        'recency_stats': {
            'min_days': int(customer_recency['recency'].min()),
            'max_days': int(customer_recency['recency'].max()),
            'mean_days': round(customer_recency['recency'].mean(), 2),
            'median_days': int(customer_recency['recency'].median()),
            'std_dev': round(customer_recency['recency'].std(), 2)
        },
        'recency_quartiles': {
            'q1_75th_percentile': round(float(quartiles['q1']), 2),
            'q2_50th_percentile': round(float(quartiles['q2']), 2),
            'q3_25th_percentile': round(float(quartiles['q3']), 2),
            'q4_max': round(float(quartiles['q4']), 2)
        },
        'customer_distribution': {
            'recent_0_30_days': len(customer_recency[customer_recency['recency'] <= 30]),
            'recent_31_60_days': len(customer_recency[(customer_recency['recency'] > 30) & (customer_recency['recency'] <= 60)]),
            'recent_61_90_days': len(customer_recency[(customer_recency['recency'] > 60) & (customer_recency['recency'] <= 90)]),
            'recent_90_plus_days': len(customer_recency[customer_recency['recency'] > 90])
        }
    }
    
    return summary


def run_recency_calculation(
    input_file: str,
    output_dir: str,
    reference_date: Optional[str] = None,
    save_summary: bool = True
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run complete recency calculation pipeline.
    
    Args:
        input_file: Path to input data file (cleaned_final.csv)
        output_dir: Directory to save recency output
        reference_date: Optional reference date as string (YYYY-MM-DD)
        save_summary: Whether to save summary JSON
        
    Returns:
        Tuple of (success, message, summary_dict)
    """
    try:
        # Load data
        df = load_data(input_file)
        
        # Step 1: Set reference date
        ref_date, date_stats = set_reference_date(df, reference_date)
        
        # Step 2: Calculate recency
        df, recency_stats = calculate_recency(df, ref_date)
        
        # Step 3: Add recency to dataset
        df = add_recency_to_dataset(df)
        
        # Extract unique customer recency data
        customer_recency = df.groupby('customerid').agg({
            'recency': 'first'  # Recency is same for all transactions of same customer
        }).reset_index()
        
        # Step 4: Calculate quartiles
        quartiles = calculate_recency_quartiles(customer_recency)
        
        # Generate summary
        summary = {
            'step': 'recency_calculation',
            'timestamp': datetime.now().isoformat(),
            'input_file': input_file,
            'output_file': os.path.join(output_dir, 'rfm_recency.csv'),
            'reference_date_stats': date_stats,
            'recency_stats': recency_stats,
            'recency_summary': generate_recency_summary(df, customer_recency, quartiles),
            'columns': list(df.columns),
            'column_count': len(df.columns)
        }
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save recency output
        recency_output_file = os.path.join(output_dir, 'rfm_recency.csv')
        customer_recency.to_csv(recency_output_file, index=False)
        
        # Save full data with recency
        full_output_file = os.path.join(output_dir, 'data_with_recency.csv')
        df.to_csv(full_output_file, index=False)
        
        # Save summary if requested
        if save_summary:
            summary_file = os.path.join(output_dir, 'recency_calculation_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
        
        return True, f"Recency calculation completed. Output saved to {recency_output_file}", summary
        
    except Exception as e:
        return False, f"Error during recency calculation: {str(e)}", {}


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python calculate_recency.py <input_file> <output_dir> [reference_date]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    reference_date = sys.argv[3] if len(sys.argv) > 3 else None
    
    success, message, summary = run_recency_calculation(input_file, output_dir, reference_date)
    
    if success:
        print(f"Success: {message}")
        print(f"Summary: {json.dumps(summary, indent=2)}")
    else:
        print(f"Error: {message}")
        sys.exit(1)