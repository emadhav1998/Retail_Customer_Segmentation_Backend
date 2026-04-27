"""
Data Exploration Script
Analyzes raw retail dataset to understand data quality and structure
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import Dict, Any, Tuple


def load_raw_dataset(file_path: str) -> pd.DataFrame:
    """
    Step 1: Load the raw dataset from CSV or Excel file.
    
    Args:
        file_path: Path to the raw data file
        
    Returns:
        DataFrame with the loaded data
    """
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == '.csv':
        df = pd.read_csv(file_path, encoding='utf-8')
    elif file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}")
    
    print(f"Loaded dataset: {len(df)} rows, {len(df.columns)} columns")
    return df


def inspect_data_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Step 2: Inspect and document data types for each column.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary mapping column names to data types
    """
    data_types = {}
    for col in df.columns:
        data_types[col] = str(df[col].dtype)
    
    return data_types


def count_missing_values(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Step 3: Count missing values for each column.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with missing value counts and percentages
    """
    missing_analysis = {}
    total_rows = len(df)
    
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        
        missing_analysis[col] = {
            "null_count": int(null_count),
            "null_percentage": round(null_percentage, 2),
            "non_null_count": int(total_rows - null_count)
        }
    
    return missing_analysis


def count_duplicates(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Step 4: Count duplicate rows in the dataset.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with duplicate statistics
    """
    total_rows = len(df)
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
    
    # Check duplicates on key columns
    key_columns = ['InvoiceNo', 'CustomerID']
    existing_key_cols = [col for col in key_columns if col in df.columns]
    
    key_duplicates = 0
    if existing_key_cols:
        key_duplicates = df.duplicated(subset=existing_key_cols).sum()
    
    return {
        "total_rows": total_rows,
        "duplicate_rows": int(duplicate_count),
        "duplicate_percentage": round(duplicate_percentage, 2),
        "key_column_duplicates": int(key_duplicates),
        "key_columns_used": existing_key_cols
    }


def analyze_invalid_values(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Step 5: Analyze invalid values in key columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with invalid value analysis
    """
    invalid_analysis = {}
    
    # Check Quantity (should be non-zero, non-negative for sales)
    if 'Quantity' in df.columns:
        invalid_qty = df[(df['Quantity'] < 0) | (df['Quantity'] == 0)]
        invalid_analysis['Quantity'] = {
            "negative_or_zero_count": int(len(invalid_qty)),
            "negative_count": int(len(df[df['Quantity'] < 0])),
            "note": "Negative quantities may indicate returns"
        }
    
    # Check UnitPrice (should be positive)
    if 'UnitPrice' in df.columns:
        invalid_price = df[df['UnitPrice'] < 0]
        invalid_analysis['UnitPrice'] = {
            "negative_count": int(len(invalid_price)),
            "zero_count": int(len(df[df['UnitPrice'] == 0])),
            "note": "Negative or zero prices are invalid"
        }
    
    # Check CustomerID (should not be null for segmentation)
    if 'CustomerID' in df.columns:
        null_customer = df['CustomerID'].isnull()
        invalid_analysis['CustomerID'] = {
            "null_count": int(null_customer.sum()),
            "null_percentage": round(null_customer.sum() / len(df) * 100, 2),
            "note": "Null CustomerID prevents customer-level analysis"
        }
    
    # Check InvoiceDate (should be valid date)
    if 'InvoiceDate' in df.columns:
        try:
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
            invalid_date = df['InvoiceDate'].isnull()
            invalid_analysis['InvoiceDate'] = {
                "invalid_count": int(invalid_date.sum()),
                "invalid_percentage": round(invalid_date.sum() / len(df) * 100, 2),
                "note": "Invalid dates need to be cleaned"
            }
        except Exception as e:
            invalid_analysis['InvoiceDate'] = {
                "error": str(e),
                "note": "Could not parse dates"
            }
    
    return invalid_analysis


def generate_column_profile(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Step 6: Generate detailed profile for each column.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with column profiles
    """
    column_profiles = {}
    
    for col in df.columns:
        profile = {
            "dtype": str(df[col].dtype),
            "unique_count": int(df[col].nunique()),
            "null_count": int(df[col].isnull().sum())
        }
        
        # Add numeric statistics for numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            profile.update({
                "mean": round(float(df[col].mean()), 2) if not df[col].isnull().all() else None,
                "median": round(float(df[col].median()), 2) if not df[col].isnull().all() else None,
                "min": round(float(df[col].min()), 2) if not df[col].isnull().all() else None,
                "max": round(float(df[col].max()), 2) if not df[col].isnull().all() else None,
                "std": round(float(df[col].std()), 2) if not df[col].isnull().all() else None
            })
        
        # Add sample values
        profile["sample_values"] = df[col].dropna().head(5).tolist()
        
        column_profiles[col] = profile
    
    return column_profiles


def export_exploration_summary(
    output_path: str,
    data_types: Dict[str, str],
    missing_analysis: Dict[str, Dict[str, Any]],
    duplicate_analysis: Dict[str, Any],
    invalid_analysis: Dict[str, Dict[str, Any]],
    column_profiles: Dict[str, Dict[str, Any]]
) -> str:
    """
    Step 7: Export exploration summary to JSON file.
    
    Args:
        output_path: Path to save the summary
        data_types: Data type information
        missing_analysis: Missing value analysis
        duplicate_analysis: Duplicate analysis
        invalid_analysis: Invalid value analysis
        column_profiles: Column profiles
        
    Returns:
        Path to the saved summary file
    """
    summary = {
        "exploration_date": datetime.now().isoformat(),
        "data_types": data_types,
        "missing_values": missing_analysis,
        "duplicates": duplicate_analysis,
        "invalid_values": invalid_analysis,
        "column_profiles": column_profiles
    }
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"Exploration summary saved to: {output_path}")
    return output_path


def run_exploration(file_path: str, output_dir: str = "reports") -> Tuple[str, Dict[str, Any]]:
    """
    Main function to run complete data exploration.
    
    Args:
        file_path: Path to the raw dataset
        output_dir: Directory to save exploration results
        
    Returns:
        Tuple of (summary_file_path, summary_dict)
    """
    print("=" * 60)
    print("Starting Data Exploration")
    print("=" * 60)
    
    # Step 1: Load dataset
    print("\n[1/7] Loading dataset...")
    df = load_raw_dataset(file_path)
    
    # Step 2: Inspect data types
    print("[2/7] Inspecting data types...")
    data_types = inspect_data_types(df)
    
    # Step 3: Count missing values
    print("[3/7] Counting missing values...")
    missing_analysis = count_missing_values(df)
    
    # Step 4: Count duplicates
    print("[4/7] Counting duplicates...")
    duplicate_analysis = count_duplicates(df)
    
    # Step 5: Analyze invalid values
    print("[5/7] Analyzing invalid values...")
    invalid_analysis = analyze_invalid_values(df)
    
    # Step 6: Generate column profiles
    print("[6/7] Generating column profiles...")
    column_profiles = generate_column_profile(df)
    
    # Step 7: Export summary
    print("[7/7] Exporting exploration summary...")
    output_path = os.path.join(output_dir, "exploration_summary.json")
    export_exploration_summary(
        output_path,
        data_types,
        missing_analysis,
        duplicate_analysis,
        invalid_analysis,
        column_profiles
    )
    
    print("\n" + "=" * 60)
    print("Data Exploration Complete!")
    print("=" * 60)
    
    # Return summary for use in API
    summary = {
        "data_types": data_types,
        "missing_values": missing_analysis,
        "duplicates": duplicate_analysis,
        "invalid_values": invalid_analysis,
        "column_profiles": column_profiles
    }
    
    return output_path, summary


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = "data/raw/online_retail.csv"
    
    output_dir = "reports"
    
    if os.path.exists(input_file):
        run_exploration(input_file, output_dir)
    else:
        print(f"Error: File not found: {input_file}")