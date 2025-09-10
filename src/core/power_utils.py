#!/usr/bin/env python3
"""
Power utility functions for REPACSS Power Measurement
Provides common functions for handling power data conversions and corrections
"""

from typing import List, Dict

# Known metrics that are typically stored in mW instead of W
MW_METRICS = [
    'drampwr', 'pkgpwr', 'gpuinputpower', 'gpuoutputpower', 
    'gpuarbitratedpowerlimit', 'gpuenforcedpowerlimit',
    'gpuinputpower', 'gpuoutputpower', 'gpuswitchpower',
    'rxinputpower', 'txoutputpower', 'systeminputpower', 'systemoutputpower'
]

# Metrics to exclude from graphs (not power consumption)
EXCLUDED_METRICS = [
    'systemheadroominstantaneous'  # This is remaining wattage, not consumption
]

# Metrics that should always be in W (not converted)
W_METRICS = [
    'computepower', 'cpupower', 'systempower', 'systempowerconsumption',
    'totalcpupower', 'totalfanpower', 'totalmemorypower', 'totalpciepower',
    'totalstoragepower', 'totalfpgapower', 'totalgpupower'
]

def get_power_conversion_sql(metric_name: str) -> str:
    """
    Get the appropriate SQL for power value conversion based on metric name
    
    Args:
        metric_name: The metric name (lowercase)
    
    Returns:
        SQL expression for the value column
    """
    metric_lower = metric_name.lower()
    
    if metric_lower in MW_METRICS:
        # Known mW metrics - always convert to W
        return "m.value / 1000.0 as value"
    elif metric_lower in W_METRICS:
        # Known W metrics - no conversion
        return "m.value"
    else:
        # Unknown metrics - use heuristic: convert if > 4000
        return """CASE 
            WHEN m.value > 4000 THEN m.value / 1000.0  -- Convert mW to W
            ELSE m.value
        END as value"""

def should_exclude_metric(metric_name: str) -> bool:
    """
    Check if a metric should be excluded from graphs
    
    Args:
        metric_name: The metric name (lowercase)
    
    Returns:
        True if metric should be excluded
    """
    return metric_name.lower() in EXCLUDED_METRICS

def get_metric_unit_info(metric_name: str) -> Dict[str, str]:
    """
    Get unit information for a metric
    
    Args:
        metric_name: The metric name
    
    Returns:
        Dictionary with unit information
    """
    metric_lower = metric_name.lower()
    
    if metric_lower in MW_METRICS:
        return {
            'original_unit': 'mW',
            'converted_unit': 'W',
            'conversion_applied': True
        }
    elif metric_lower in W_METRICS:
        return {
            'original_unit': 'W',
            'converted_unit': 'W',
            'conversion_applied': False
        }
    else:
        return {
            'original_unit': 'Unknown (mW or W)',
            'converted_unit': 'W',
            'conversion_applied': 'Conditional (>4000)'
        }

def create_power_query_with_conversion(base_query: str, metric_name: str) -> str:
    """
    Create a power query with appropriate unit conversion
    
    Args:
        base_query: The base SQL query
        metric_name: The metric name for determining conversion
    
    Returns:
        Modified query with unit conversion
    """
    conversion_sql = get_power_conversion_sql(metric_name)
    return base_query.replace('m.value', conversion_sql)

def validate_power_values(df, metric_name: str, threshold: float = 5000.0) -> Dict[str, any]:
    """
    Validate power values and provide statistics
    
    Args:
        df: DataFrame with power data
        metric_name: The metric name
        threshold: Threshold for flagging suspicious values
    
    Returns:
        Dictionary with validation results
    """
    if df.empty or 'value' not in df.columns:
        return {'valid': False, 'reason': 'No data or missing value column'}
    
    values = df['value']
    max_val = values.max()
    min_val = values.min()
    mean_val = values.mean()
    
    # Count values that might be in mW (very high values)
    high_values = values[values > threshold].count()
    
    unit_info = get_metric_unit_info(metric_name)
    
    return {
        'valid': True,
        'max_value': max_val,
        'min_value': min_val,
        'mean_value': mean_val,
        'high_value_count': high_values,
        'unit_info': unit_info,
        'suspicious': max_val > threshold and unit_info['conversion_applied'] is False
    }
