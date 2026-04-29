"""
Data Cleaning Step 2 Script
Removes invalid quantities, prices, and handles date parsing
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
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


def remove_non_positive_quantities(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 1: Remove rows with non-positive (zero or negative) quantities.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, stats dict)
    """
    initial_count = len(df)
    
    quantity_col = 'quantity'
    
    if quantity_col in df.columns:
        # Count before removal
        non_positive_before = (df[quantity_col] <= 0).sum()
        
        # Keep only positive quantities
        df = df[df[quantity_col] > 0]
        
        removed = initial_count - len(df)
        
        stats = {
            'initial_rows': initial_count,
            'non_positive_quantities_removed': removed,
            'remaining_rows': len(df),
            'removal_percentage': round((removed / initial_count * 100), 2) if initial_count > 0 else 0
        }
    else:
        stats = {
            'initial_rows': initial_count,
            'non_positive_quantities_removed': 0,
            'remaining_rows': len(df),
            'removal_percentage': 0
        }
    
    return df, stats


def remove_non_positive_unit_prices(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 2: Remove rows with non-positive (zero or negative) unit prices.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, stats dict)
    """
    initial_count = len(df)
    
    unit_price_col = 'unitprice'
    
    if unit_price_col in df.columns:
        # Count before removal
        non_positive_before = (df[unit_price_col] <= 0).sum()
        
        # Keep only positive unit prices
        df = df[df[unit_price_col] > 0]
        
        removed = initial_count - len(df)
        
        stats = {
            'initial_rows': initial_count,
            'non_positive_prices_removed': removed,
            'remaining_rows': len(df),
            'removal_percentage': round((removed / initial_count * 100), 2) if initial_count > 0 else 0
        }
    else:
        stats = {
            'initial_rows': initial_count,
            'non_positive_prices_removed': 0,
            'remaining_rows': len(df),
            'removal_percentage': 0
        }
    
    return df, stats


def parse_transaction_dates(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 3: Parse transaction dates and convert to datetime.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, stats dict)
    """
    initial_count = len(df)
    
    date_col = 'invoicedate'
    parsed_count = 0
    parse_errors = 0
    
    if date_col in df.columns:
        # Try to parse dates with multiple format attempts
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Count successfully parsed
        parsed_count = df[date_col].notna().sum()
        parse_errors = df[date_col].isna().sum()
        
        stats = {
            'initial_rows': initial_count,
            'dates_parsed': parsed_count,
            'parse_errors': parse_errors,
            'success_rate': round((parsed_count / initial_count * 100), 2) if initial_count > 0 else 0
        }
    else:
        stats = {
            'initial_rows': initial_count,
            'dates_parsed': 0,
            'parse_errors': initial_count,
            'success_rate': 0
        }
    
    return df, stats


def drop_invalid_dates(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 4: Drop rows with invalid (null or future) dates.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, stats dict)
    """
    initial_count = len(df)
    
    date_col = 'invoicedate'
    
    if date_col in df.columns:
        # Count null/invalid dates before removal
        invalid_before = df[date_col].isna().sum()
        
        # Get the reference date (max date in dataset or current date)
        # Using a reasonable reference: dataset max date or 2011-12-31 (common in retail datasets)
        reference_date = pd.Timestamp('2011-12-31')
        
        # Drop null dates
        df = df[df[date_col].notna()]
        
        # Drop future dates (if any)
        df = df[df[date_col] <= reference_date]
        
        removed = initial_count - len(df)
        
        stats = {
            'initial_rows': initial_count,
            'invalid_dates_removed': removed,
            'remaining_rows': len(df),
            'removal_percentage': round((removed / initial_count * 100), 2) if initial_count > 0 else 0
        }
    else:
        stats = {
            'initial_rows': initial_count,
            'invalid_dates_removed': 0,
            'remaining_rows': len(df),
            'removal_percentage': 0
        }
    
    return df, stats


def remove_cancelled_orders(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 5: Remove cancelled orders (InvoiceNo starting with 'C').
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, stats dict)
    """
    initial_count = len(df)
    
    invoice_col = 'invoiceno'
    
    if invoice_col in df.columns:
        # Count cancelled orders before removal
        cancelled_before = df[invoice_col].astype(str).str.startswith('C').sum()
        
        # Keep only non-cancelled orders
        df = df[~df[invoice_col].astype(str).str.startswith('C')]
        
        removed = initial_count - len(df)
        
        stats = {
            'initial_rows': initial_count,
            'cancelled_orders_removed': removed,
            'remaining_rows': len(df),
            'removal_percentage': round((removed / initial_count * 100), 2) if initial_count > 0 else 0
        }
    else:
        stats = {
            'initial_rows': initial_count,
            'cancelled_orders_removed': 0,
            'remaining_rows': len(df),
            'removal_percentage': 0
        }
    
    return df, stats


def calculate_total_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 6: Calculate total revenue per transaction.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with total revenue column
    """
    if 'quantity' in df.columns and 'unitprice' in df.columns:
        df['total_revenue'] = df['quantity'] * df['unitprice']
    
    return df


def run_cleaning_step2(
    input_file: str,
    output_dir: str,
    save_summary: bool = True
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run complete cleaning step 2 pipeline.
    
    Args:
        input_file: Path to input data file (cleaned_step1.csv)
        output_dir: Directory to save cleaned output
        save_summary: Whether to save summary JSON
        
    Returns:
        Tuple of (success, message, summary_dict)
    """
    try:
        # Load data
        df = load_data(input_file)
        initial_rows = len(df)
        
        # Step 1: Remove non-positive quantities
        df, quantity_stats = remove_non_positive_quantities(df)
        
        # Step 2: Remove non-positive unit prices
        df, price_stats = remove_non_positive_unit_prices(df)
        
        # Step 3: Parse transaction dates
        df, date_parse_stats = parse_transaction_dates(df)
        
        # Step 4: Drop invalid dates
        df, invalid_date_stats = drop_invalid_dates(df)
        
        # Step 5: Remove cancelled orders
        df, cancelled_stats = remove_cancelled_orders(df)
        
        # Step 6: Calculate total revenue
        df = calculate_total_revenue(df)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save cleaned data
        output_file = os.path.join(output_dir, 'cleaned_final.csv')
        df.to_csv(output_file, index=False)
        
        # Calculate final statistics
        final_rows = len(df)
        total_removed = initial_rows - final_rows
        
        summary = {
            'step': 'cleaning_step2',
            'timestamp': datetime.now().isoformat(),
            'input_file': input_file,
            'output_file': output_file,
            'initial_rows': initial_rows,
            'final_rows': final_rows,
            'total_rows_removed': total_removed,
            'removal_percentage': round((total_removed / initial_rows * 100), 2) if initial_rows > 0 else 0,
            'quantity_stats': quantity_stats,
            'price_stats': price_stats,
            'date_parse_stats': date_parse_stats,
            'invalid_date_stats': invalid_date_stats,
            'cancelled_stats': cancelled_stats,
            'columns': list(df.columns),
            'column_count': len(df.columns),
            'date_range': {
                'min': df['invoicedate'].min().isoformat() if 'invoicedate' in df.columns and df['invoicedate'].notna().any() else None,
                'max': df['invoicedate'].max().isoformat() if 'invoicedate' in df.columns and df['invoicedate'].notna().any() else None
            }
        }
        
        # Save summary if requested
        if save_summary:
            summary_file = os.path.join(output_dir, 'cleaning_step2_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
        
        return True, f"Cleaning step 2 completed. Output saved to {output_file}", summary
        
    except Exception as e:
        return False, f"Error during cleaning: {str(e)}", {}


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python clean_data_step2.py <input_file> <output_dir>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    success, message, summary = run_cleaning_step2(input_file, output_dir)
    
    if success:
        print(f"Success: {message}")
        print(f"Summary: {json.dumps(summary, indent=2)}")
    else:
        print(f"Error: {message}")
        sys.exit(1)