"""
Data Cleaning Step 1 Script
Standardizes column names, removes duplicates, and handles missing customer identifiers
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional


# Required columns that must be present
REQUIRED_COLUMNS = [
    'InvoiceNo', 'StockCode', 'Description', 'Quantity',
    'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'
]


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load data from CSV or Excel file.
    
    Args:
        file_path: Path to the data file
        
    Returns:
        DataFrame with loaded data
    """
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == '.csv':
        df = pd.read_csv(file_path, encoding='utf-8')
    elif file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}")
    
    return df


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 1: Standardize column names to lowercase with underscores.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with standardized column names
    """
    # Create a mapping for common column name variations
    column_mapping = {}
    
    for col in df.columns:
        # Convert to lowercase and replace spaces/special chars with underscores
        new_col = col.lower().strip().replace(' ', '_').replace('-', '_')
        # Remove any non-alphanumeric characters except underscores
        new_col = ''.join(c if c.isalnum() or c == '_' else '' for c in new_col)
        # Remove consecutive underscores
        while '__' in new_col:
            new_col = new_col.replace('__', '_')
        column_mapping[col] = new_col
    
    df = df.rename(columns=column_mapping)
    
    return df


def remove_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 2: Remove duplicate rows.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, stats dict)
    """
    initial_count = len(df)
    
    # Remove exact duplicate rows
    df = df.drop_duplicates()
    
    duplicate_count = initial_count - len(df)
    
    stats = {
        'initial_rows': initial_count,
        'duplicate_rows_removed': duplicate_count,
        'remaining_rows': len(df),
        'duplicate_percentage': round((duplicate_count / initial_count * 100), 2) if initial_count > 0 else 0
    }
    
    return df, stats


def remove_missing_customer_identifiers(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 3: Remove rows with missing customer identifiers.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, stats dict)
    """
    initial_count = len(df)
    
    # Check for missing CustomerID (can be NaN, None, or empty string)
    customer_id_col = 'customerid'
    
    if customer_id_col in df.columns:
        # Count missing before removal
        missing_before = df[customer_id_col].isna().sum()
        
        # Also check for empty strings or "nan" string values
        df = df[df[customer_id_col].notna()]
        df = df[df[customer_id_col] != '']
        df = df[df[customer_id_col].astype(str).str.lower() != 'nan']
        
        missing_removed = initial_count - len(df)
        
        stats = {
            'initial_rows': initial_count,
            'missing_customerid_removed': missing_removed,
            'remaining_rows': len(df),
            'missing_percentage': round((missing_removed / initial_count * 100), 2) if initial_count > 0 else 0
        }
    else:
        stats = {
            'initial_rows': initial_count,
            'missing_customerid_removed': 0,
            'remaining_rows': len(df),
            'missing_percentage': 0
        }
    
    return df, stats


def validate_required_columns(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
    """
    Validate that required columns exist after standardization.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Map original required columns to standardized names
    standardized_required = [col.lower().replace(' ', '_').replace('-', '_') for col in REQUIRED_COLUMNS]
    
    missing_cols = []
    for col in standardized_required:
        if col not in df.columns:
            missing_cols.append(col)
    
    if missing_cols:
        return False, f"Missing required columns: {', '.join(missing_cols)}"
    
    return True, None


def run_cleaning_step1(
    input_file: str,
    output_dir: str,
    save_summary: bool = True
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run complete cleaning step 1 pipeline.
    
    Args:
        input_file: Path to input data file
        output_dir: Directory to save cleaned output
        save_summary: Whether to save summary JSON
        
    Returns:
        Tuple of (success, message, summary_dict)
    """
    try:
        # Load data
        df = load_data(input_file)
        initial_rows = len(df)
        
        # Step 1: Standardize column names
        df = standardize_column_names(df)
        
        # Validate required columns exist
        is_valid, error_msg = validate_required_columns(df)
        if not is_valid:
            return False, error_msg, {}
        
        # Step 2: Remove duplicates
        df, duplicate_stats = remove_duplicates(df)
        
        # Step 3: Remove missing customer identifiers
        df, customerid_stats = remove_missing_customer_identifiers(df)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save cleaned data
        output_file = os.path.join(output_dir, 'cleaned_step1.csv')
        df.to_csv(output_file, index=False)
        
        # Calculate final statistics
        final_rows = len(df)
        total_removed = initial_rows - final_rows
        
        summary = {
            'step': 'cleaning_step1',
            'timestamp': datetime.now().isoformat(),
            'input_file': input_file,
            'output_file': output_file,
            'initial_rows': initial_rows,
            'final_rows': final_rows,
            'total_rows_removed': total_removed,
            'removal_percentage': round((total_removed / initial_rows * 100), 2) if initial_rows > 0 else 0,
            'duplicate_stats': duplicate_stats,
            'customerid_stats': customerid_stats,
            'columns': list(df.columns),
            'column_count': len(df.columns)
        }
        
        # Save summary if requested
        if save_summary:
            summary_file = os.path.join(output_dir, 'cleaning_step1_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
        
        return True, f"Cleaning step 1 completed. Output saved to {output_file}", summary
        
    except Exception as e:
        return False, f"Error during cleaning: {str(e)}", {}


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python clean_data_step1.py <input_file> <output_dir>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    success, message, summary = run_cleaning_step1(input_file, output_dir)
    
    if success:
        print(f"Success: {message}")
        print(f"Summary: {json.dumps(summary, indent=2)}")
    else:
        print(f"Error: {message}")
        sys.exit(1)