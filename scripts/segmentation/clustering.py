"""
Clustering Script
Apply K-Means clustering on scaled RFM metrics
"""
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans


def load_input_data(file_path: str) -> pd.DataFrame:
    """Load customer segments or RFM scores file."""
    return pd.read_csv(file_path, encoding='utf-8')


def scale_rfm_metrics(df: pd.DataFrame) -> Tuple[pd.DataFrame, StandardScaler]:
    """
    Scale recency, frequency, and monetary values using StandardScaler.

    Args:
        df: DataFrame with recency, frequency, monetary columns

    Returns:
        Tuple of (DataFrame with scaled values, fitted scaler)
    """
    df = df.copy()

    # Select RFM metric columns (values, not scores)
    rfm_columns = ['recency', 'frequency', 'monetary']
    
    # Check which columns exist
    available_columns = [col for col in rfm_columns if col in df.columns]
    
    if not available_columns:
        raise ValueError(f"No RFM metric columns found. Need at least one of: {rfm_columns}")

    # Extract RFM metrics
    rfm_metrics = df[available_columns].copy()
    
    # Handle missing values
    rfm_metrics = rfm_metrics.fillna(rfm_metrics.median())

    # Initialize and fit scaler
    scaler = StandardScaler()
    scaled_metrics = scaler.fit_transform(rfm_metrics)

    # Create scaled columns
    for i, col in enumerate(available_columns):
        df[f'{col}_scaled'] = scaled_metrics[:, i]

    return df, scaler


def apply_kmeans_clustering(
    df: pd.DataFrame,
    n_clusters: int = 4,
    random_state: int = 42
) -> Tuple[pd.DataFrame, KMeans]:
    """
    Apply K-Means clustering on scaled RFM metrics.

    Args:
        df: DataFrame with scaled RFM columns
        n_clusters: Number of clusters
        random_state: Random seed for reproducibility

    Returns:
        Tuple of (DataFrame with cluster assignments, fitted KMeans model)
    """
    df = df.copy()

    # Get scaled RFM columns
    scaled_columns = [col for col in df.columns if col.endswith('_scaled')]
    
    if not scaled_columns:
        raise ValueError("No scaled RFM columns found. Run scale_rfm_metrics first.")

    # Prepare data for clustering
    X = df[scaled_columns].values

    # Initialize and fit KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
    clusters = kmeans.fit_predict(X)

    # Add cluster assignments
    df['cluster_id'] = clusters

    return df, kmeans


def assign_cluster_labels(
    df: pd.DataFrame,
    kmeans: KMeans
) -> pd.DataFrame:
    """
    Assign meaningful labels to clusters based on centroid characteristics.

    Args:
        df: DataFrame with cluster_id column
        kmeans: Fitted KMeans model

    Returns:
        DataFrame with cluster_label column
    """
    df = df.copy()

    # Get scaled columns for centroid analysis
    scaled_columns = [col for col in df.columns if col.endswith('_scaled')]

    # Analyze centroids
    centroids = kmeans.cluster_centers_
    centroid_df = pd.DataFrame(centroids, columns=scaled_columns)

    # Map centroid characteristics to labels
    labels = {}
    for cluster_id in range(kmeans.n_clusters):
        centroid = centroid_df.loc[cluster_id]
        
        # Determine label based on centroid values
        # Positive = high, Negative = low
        recency_val = centroid.get('recency_scaled', 0)
        frequency_val = centroid.get('frequency_scaled', 0)
        monetary_val = centroid.get('monetary_scaled', 0)

        # Assign labels based on characteristics
        if frequency_val > 0 and monetary_val > 0:
            if recency_val > 0:
                label = f"Cluster_{cluster_id}_Active_High_Value"
            else:
                label = f"Cluster_{cluster_id}_Inactive_High_Value"
        elif frequency_val > 0 and monetary_val <= 0:
            label = f"Cluster_{cluster_id}_Frequent_Low_Value"
        elif frequency_val <= 0 and monetary_val > 0:
            label = f"Cluster_{cluster_id}_Infrequent_High_Value"
        else:
            label = f"Cluster_{cluster_id}_Low_Activity"

        labels[cluster_id] = label

    # Add cluster labels
    df['cluster_label'] = df['cluster_id'].map(labels)

    return df


def calculate_cluster_metrics(df: pd.DataFrame, kmeans: KMeans) -> Dict[str, Any]:
    """
    Calculate key metrics for each cluster.

    Args:
        df: DataFrame with cluster assignments
        kmeans: Fitted KMeans model

    Returns:
        Dictionary with cluster metrics
    """
    metrics = {}
    scaled_columns = [col for col in df.columns if col.endswith('_scaled')]

    for cluster_id in range(kmeans.n_clusters):
        cluster_data = df[df['cluster_id'] == cluster_id]

        # Get original metrics
        original_columns = [col.replace('_scaled', '') for col in scaled_columns]
        
        cluster_stats = {
            'customer_count': len(cluster_data),
            'percentage_of_total': round((len(cluster_data) / len(df) * 100), 2),
        }

        # Add statistics for each original metric
        for orig_col in original_columns:
            if orig_col in cluster_data.columns:
                cluster_stats[f'avg_{orig_col}'] = round(cluster_data[orig_col].mean(), 2)
                cluster_stats[f'min_{orig_col}'] = round(cluster_data[orig_col].min(), 2)
                cluster_stats[f'max_{orig_col}'] = round(cluster_data[orig_col].max(), 2)

        metrics[cluster_id] = cluster_stats

    return metrics


def generate_centroid_summary(
    kmeans: KMeans,
    metrics: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate centroid summary for visualization/analysis.

    Args:
        kmeans: Fitted KMeans model
        metrics: Cluster metrics dictionary

    Returns:
        Dictionary with centroid information
    """
    summary = {}

    for cluster_id in range(kmeans.n_clusters):
        summary[cluster_id] = {
            'metrics': metrics.get(cluster_id, {}),
            'inertia_contribution': round(float(kmeans.inertia_ / kmeans.n_clusters), 2),
        }

    return summary


def run_clustering(
    input_file: str,
    output_dir: str,
    n_clusters: int = 4,
    random_state: int = 42,
    save_summary: bool = True
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run complete K-Means clustering pipeline.

    Args:
        input_file: Path to customer_segments.csv or rfm_scores.csv
        output_dir: Directory to save output files
        n_clusters: Number of clusters for K-Means
        random_state: Random seed for reproducibility
        save_summary: Whether to save summary JSON

    Returns:
        Tuple of (success, message, summary_dict)
    """
    try:
        # Load input data
        if not os.path.exists(input_file):
            return False, f"Input file not found: {input_file}", {}

        df = load_input_data(input_file)

        # Validate required columns
        required = ['recency', 'frequency', 'monetary']
        missing = [c for c in required if c not in df.columns]
        if missing:
            return False, f"Missing required columns: {', '.join(missing)}", {}

        # Step 1: Scale RFM metrics
        df, scaler = scale_rfm_metrics(df)

        # Step 2: Apply K-Means clustering
        df, kmeans = apply_kmeans_clustering(df, n_clusters=n_clusters, random_state=random_state)

        # Step 3: Assign cluster labels
        df = assign_cluster_labels(df, kmeans)

        # Step 4: Calculate cluster metrics
        metrics = calculate_cluster_metrics(df, kmeans)

        # Step 5: Generate centroid summary
        centroid_summary = generate_centroid_summary(kmeans, metrics)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Save customer clusters
        output_file = os.path.join(output_dir, 'customer_clusters.csv')
        df.to_csv(output_file, index=False)

        # Generate summary
        summary = {
            'step': 'clustering',
            'timestamp': datetime.now().isoformat(),
            'input_file': input_file,
            'output_file': output_file,
            'n_clusters': n_clusters,
            'total_customers': len(df),
            'cluster_metrics': metrics,
            'centroid_summary': centroid_summary,
            'kmeans_inertia': float(kmeans.inertia_),
            'columns': list(df.columns),
            'column_count': len(df.columns),
        }

        if save_summary:
            summary_file = os.path.join(output_dir, 'clustering_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)

        return True, f"Clustering completed. Output saved to {output_file}", summary

    except Exception as e:
        return False, f"Error during clustering: {str(e)}", {}


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python clustering.py <input_file> <output_dir> [n_clusters]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    n_clusters = int(sys.argv[3]) if len(sys.argv) > 3 else 4

    success, message, summary = run_clustering(input_file, output_dir, n_clusters=n_clusters)

    if success:
        print(f"Success: {message}")
        print(json.dumps(summary, indent=2))
    else:
        print(f"Error: {message}")
        sys.exit(1)
