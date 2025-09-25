"""
Query helper utilities for REPACSS Power Measurement
"""

from typing import List
from constants.metrics import EXCLUDED_METRICS


def get_power_conversion_sql(unit: str) -> str:
    """
    Get the appropriate SQL for power value conversion based on unit
    
    Args:
        unit: The unit from metrics_definition table (e.g., 'mW', 'kW', 'W')
    
    Returns:
        SQL expression for the value column converted to Watts
    """
    unit_lower = unit.lower() if unit else 'w'
    
    if unit_lower == 'mw':
        return "m.value / 1000.0 as value"
    elif unit_lower == 'kw':
        return "m.value * 1000.0 as value"
    elif unit_lower == 'w':
        return "m.value as value"
    else:
        # Default to W if unit is unknown
        return "m.value as value"


def should_exclude_metric(metric_name: str) -> bool:
    """
    Check if a metric should be excluded from analysis
    
    Args:
        metric_name: Name of the metric to check
    
    Returns:
        True if metric should be excluded, False otherwise
    """
    return metric_name.lower() in [m.lower() for m in EXCLUDED_METRICS]


def create_power_query_with_conversion(base_query: str, unit: str) -> str:
    """
    Create a power query with unit conversion applied
    
    Args:
        base_query: Base SQL query
        unit: Unit to convert from
    
    Returns:
        Modified query with unit conversion
    """
    conversion_sql = get_power_conversion_sql(unit)
    return base_query.replace('m.value', conversion_sql)
