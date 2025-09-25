#!/usr/bin/env python3
"""
Data processing utilities for REPACSS Power Measurement
Handles data manipulation, cleaning, and analysis
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
import numpy as np

from .conversions import convert_power_series_to_watts


def process_power_data(df: pd.DataFrame, hostname: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    """
    Process power data with energy calculations and power conversions.
    
    Args:
        df: Raw power data DataFrame
        hostname: Node hostname
        start_time: Start timestamp
        end_time: End timestamp
    
    Returns:
        Processed DataFrame with energy calculations
    """
    if df.empty:
        return df
    
    processed_df = df.copy()
    
    # Sort by timestamp
    processed_df = processed_df.sort_values('timestamp')
    
    # Convert power to Watts if units column exists
    if 'units' in processed_df.columns:
        processed_df['power_w'] = convert_power_series_to_watts(processed_df['value'], processed_df['units'].iloc[0])
    else:
        processed_df['power_w'] = processed_df['value']
    
    # Calculate time differences
    processed_df['time_diff_seconds'] = processed_df['timestamp'].diff().dt.total_seconds()
    
    # Calculate average power between consecutive readings
    processed_df['avg_power_w'] = (processed_df['power_w'] + processed_df['power_w'].shift(1)) / 2.0
    
    # Calculate energy for each interval (kWh)
    processed_df['energy_interval_kwh'] = (
        processed_df['avg_power_w'] * processed_df['time_diff_seconds']
    ) / 3_600_000.0
    
    # Handle first row: set NaN for time-based calculations
    if len(processed_df) > 0:
        processed_df.loc[0, 'time_diff_seconds'] = pd.NA
        processed_df.loc[0, 'avg_power_w'] = pd.NA
        processed_df.loc[0, 'energy_interval_kwh'] = pd.NA
    
    # Calculate cumulative energy
    processed_df['cumulative_energy_kwh'] = 0.0
    if len(processed_df) > 0:
        processed_df.loc[0, 'cumulative_energy_kwh'] = 0.0
    
    # Calculate cumulative energy for remaining rows
    for i in range(1, len(processed_df)):
        if pd.notna(processed_df['time_diff_seconds'].iloc[i]) and pd.notna(processed_df['energy_interval_kwh'].iloc[i]):
            processed_df.loc[i, 'cumulative_energy_kwh'] = (
                processed_df['cumulative_energy_kwh'].iloc[i-1] + 
                processed_df['energy_interval_kwh'].iloc[i]
            )
    
    return processed_df


def clean_power_data(df: pd.DataFrame, remove_outliers: bool = True, 
                    outlier_threshold: float = 3.0) -> pd.DataFrame:
    """
    Clean power data by removing outliers and invalid values.
    
    Args:
        df: Power data DataFrame
        remove_outliers: Whether to remove statistical outliers
        outlier_threshold: Z-score threshold for outlier detection
    
    Returns:
        Cleaned DataFrame
    """
    cleaned_df = df.copy()
    
    # Remove rows with invalid power values
    if 'power_w' in cleaned_df.columns:
        # Remove negative power values (unless they represent power generation)
        cleaned_df = cleaned_df[cleaned_df['power_w'] >= 0]
        
        # Remove extremely high power values (likely data errors)
        cleaned_df = cleaned_df[cleaned_df['power_w'] <= 10000]  # 10kW max per node
    
    # Remove rows with invalid timestamps
    if 'timestamp' in cleaned_df.columns:
        cleaned_df = cleaned_df.dropna(subset=['timestamp'])
        # Remove future timestamps
        now = pd.Timestamp.now()
        cleaned_df = cleaned_df[cleaned_df['timestamp'] <= now]
    
    # Remove statistical outliers if requested
    if remove_outliers and 'power_w' in cleaned_df.columns and len(cleaned_df) > 10:
        z_scores = np.abs((cleaned_df['power_w'] - cleaned_df['power_w'].mean()) / cleaned_df['power_w'].std())
        cleaned_df = cleaned_df[z_scores < outlier_threshold]
    
    return cleaned_df


def aggregate_power_data(df: pd.DataFrame, group_by: str = 'hostname', 
                        time_window: str = '1H') -> pd.DataFrame:
    """
    Aggregate power data by time windows and groups.
    
    Args:
        df: Power data DataFrame
        group_by: Column to group by (default: 'hostname')
        time_window: Pandas time window string (e.g., '1H', '30T', '1D')
    
    Returns:
        Aggregated DataFrame
    """
    if df.empty:
        return df
    
    # Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Group by specified column and time window
    grouped = df.groupby([group_by, pd.Grouper(key='timestamp', freq=time_window)])
    
    # Aggregate power data
    aggregated = grouped.agg({
        'power_w': ['mean', 'max', 'min', 'std', 'count'],
        'value': ['mean', 'max', 'min'],
        'cumulative_energy_kwh': 'last'  # Take the last cumulative value
    }).reset_index()
    
    # Flatten column names
    aggregated.columns = ['_'.join(col).strip() if col[1] else col[0] for col in aggregated.columns]
    
    return aggregated


def calculate_power_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate comprehensive power statistics.
    
    Args:
        df: Power data DataFrame
    
    Returns:
        Dictionary with power statistics
    """
    if df.empty or 'power_w' not in df.columns:
        return {}
    
    stats = {
        'count': len(df),
        'mean_power_w': df['power_w'].mean(),
        'median_power_w': df['power_w'].median(),
        'std_power_w': df['power_w'].std(),
        'min_power_w': df['power_w'].min(),
        'max_power_w': df['power_w'].max(),
        'q25_power_w': df['power_w'].quantile(0.25),
        'q75_power_w': df['power_w'].quantile(0.75),
    }
    
    # Add energy statistics if available
    if 'cumulative_energy_kwh' in df.columns and not df['cumulative_energy_kwh'].empty:
        stats['total_energy_kwh'] = df['cumulative_energy_kwh'].iloc[-1]
    
    # Add time range statistics
    if 'timestamp' in df.columns and len(df) > 1:
        time_range = df['timestamp'].max() - df['timestamp'].min()
        stats['time_range_hours'] = time_range.total_seconds() / 3600
        stats['data_points_per_hour'] = len(df) / stats['time_range_hours'] if stats['time_range_hours'] > 0 else 0
    
    return stats


def detect_power_anomalies(df: pd.DataFrame, threshold_std: float = 2.0) -> pd.DataFrame:
    """
    Detect power consumption anomalies.
    
    Args:
        df: Power data DataFrame
        threshold_std: Standard deviation threshold for anomaly detection
    
    Returns:
        DataFrame with anomaly flags
    """
    if df.empty or 'power_w' not in df.columns:
        return df
    
    df_with_anomalies = df.copy()
    
    # Calculate rolling statistics
    window_size = min(50, len(df) // 10)  # Adaptive window size
    if window_size > 1:
        df_with_anomalies['power_rolling_mean'] = df_with_anomalies['power_w'].rolling(window=window_size, center=True).mean()
        df_with_anomalies['power_rolling_std'] = df_with_anomalies['power_w'].rolling(window=window_size, center=True).std()
        
        # Detect anomalies
        df_with_anomalies['is_anomaly'] = (
            np.abs(df_with_anomalies['power_w'] - df_with_anomalies['power_rolling_mean']) > 
            (threshold_std * df_with_anomalies['power_rolling_std'])
        )
    else:
        df_with_anomalies['is_anomaly'] = False
    
    return df_with_anomalies


def merge_power_dataframes(dataframes: List[pd.DataFrame], 
                          merge_on: List[str] = ['timestamp', 'hostname']) -> pd.DataFrame:
    """
    Merge multiple power data DataFrames.
    
    Args:
        dataframes: List of DataFrames to merge
        merge_on: Columns to merge on
    
    Returns:
        Merged DataFrame
    """
    if not dataframes:
        return pd.DataFrame()
    
    if len(dataframes) == 1:
        return dataframes[0]
    
    # Start with the first DataFrame
    merged_df = dataframes[0].copy()
    
    # Merge with remaining DataFrames
    for df in dataframes[1:]:
        if not df.empty:
            merged_df = pd.merge(merged_df, df, on=merge_on, how='outer', suffixes=('', '_y'))
    
    return merged_df


def validate_power_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate power data quality and completeness.
    
    Args:
        df: Power data DataFrame
    
    Returns:
        Dictionary with validation results
    """
    validation_results = {
        'is_valid': True,
        'issues': [],
        'warnings': [],
        'statistics': {}
    }
    
    if df.empty:
        validation_results['is_valid'] = False
        validation_results['issues'].append('DataFrame is empty')
        return validation_results
    
    # Check required columns
    required_columns = ['timestamp', 'value']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_results['is_valid'] = False
        validation_results['issues'].append(f'Missing required columns: {missing_columns}')
    
    # Check data types
    if 'timestamp' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            validation_results['warnings'].append('Timestamp column is not datetime type')
    
    # Check for null values
    null_counts = df.isnull().sum()
    if null_counts.any():
        validation_results['warnings'].append(f'Null values found: {null_counts[null_counts > 0].to_dict()}')
    
    # Check power value ranges
    if 'value' in df.columns:
        power_values = pd.to_numeric(df['value'], errors='coerce')
        if power_values.isnull().any():
            validation_results['warnings'].append('Non-numeric values found in power data')
        
        if (power_values < 0).any():
            validation_results['warnings'].append('Negative power values found')
        
        if (power_values > 10000).any():
            validation_results['warnings'].append('Extremely high power values found (>10kW)')
    
    # Calculate basic statistics
    validation_results['statistics'] = {
        'total_records': len(df),
        'unique_hostnames': df['hostname'].nunique() if 'hostname' in df.columns else 0,
        'time_range_hours': 0
    }
    
    if 'timestamp' in df.columns and len(df) > 1:
        time_range = df['timestamp'].max() - df['timestamp'].min()
        validation_results['statistics']['time_range_hours'] = time_range.total_seconds() / 3600
    
    return validation_results
