#!/usr/bin/env python3
"""
Unit conversion utilities for REPACSS Power Measurement
Handles power unit conversions and data transformations
"""

import pandas as pd
from typing import Dict, Any


def convert_power_series_to_watts(power_series: pd.Series, unit: str) -> pd.Series:
    """
    Convert a pandas Series of power values to Watts based on unit.

    Args:
        power_series: Pandas Series containing power values
        unit: The unit from metrics_definition table (e.g., 'mW', 'kW', 'W')

    Returns:
        Pandas Series of power in Watts
    """
    unit_lower = unit.lower() if unit else 'w'
    series_float = power_series.astype(float)

    if unit_lower == 'mw':
        return series_float / 1000.0
    elif unit_lower == 'kw':
        return series_float * 1000.0
    elif unit_lower == 'w':
        return series_float
    else:
        # Default to W if unit is unknown
        return series_float


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


def get_metric_unit_info(unit: str) -> Dict[str, str]:
    """
    Get unit information for a metric based on its unit
    
    Args:
        unit: The unit from metrics_definition table
    
    Returns:
        Dictionary with unit information
    """
    unit_lower = unit.lower() if unit else 'w'
    
    if unit_lower == 'mw':
        return {
            'original_unit': 'mW',
            'converted_unit': 'W',
            'conversion_applied': True
        }
    elif unit_lower == 'kw':
        return {
            'original_unit': 'kW',
            'converted_unit': 'W',
            'conversion_applied': True
        }
    elif unit_lower == 'w':
        return {
            'original_unit': 'W',
            'converted_unit': 'W',
            'conversion_applied': False
        }
    else:
        return {
            'original_unit': unit or 'Unknown',
            'converted_unit': 'W',
            'conversion_applied': False
        }


def create_power_query_with_conversion(base_query: str, unit: str) -> str:
    """
    Create a power query with appropriate unit conversion
    
    Args:
        base_query: The base SQL query
        unit: The unit from metrics_definition table
    
    Returns:
        Modified query with unit conversion
    """
    conversion_sql = get_power_conversion_sql(unit)
    return base_query.replace('m.value', conversion_sql)


def convert_temperature_series(temperature_series: pd.Series, from_unit: str, to_unit: str = 'C') -> pd.Series:
    """
    Convert temperature values between different units.
    
    Args:
        temperature_series: Pandas Series containing temperature values
        from_unit: Source unit ('C', 'F', 'K')
        to_unit: Target unit ('C', 'F', 'K')
    
    Returns:
        Pandas Series of converted temperature values
    """
    if from_unit == to_unit:
        return temperature_series
    
    series_float = temperature_series.astype(float)
    
    # Convert to Celsius first
    if from_unit.upper() == 'F':
        celsius = (series_float - 32) * 5/9
    elif from_unit.upper() == 'K':
        celsius = series_float - 273.15
    else:  # Already Celsius
        celsius = series_float
    
    # Convert from Celsius to target unit
    if to_unit.upper() == 'F':
        return celsius * 9/5 + 32
    elif to_unit.upper() == 'K':
        return celsius + 273.15
    else:  # Celsius
        return celsius


def convert_energy_joules_to_kwh(joules: float) -> float:
    """
    Convert energy from Joules to kWh.
    
    Args:
        joules: Energy in Joules
    
    Returns:
        Energy in kWh
    """
    return joules / 3_600_000.0


def convert_energy_kwh_to_joules(kwh: float) -> float:
    """
    Convert energy from kWh to Joules.
    
    Args:
        kwh: Energy in kWh
    
    Returns:
        Energy in Joules
    """
    return kwh * 3_600_000.0


def normalize_power_data(df: pd.DataFrame, power_column: str = 'value', 
                        unit_column: str = 'units') -> pd.DataFrame:
    """
    Normalize power data by converting all values to Watts.
    
    Args:
        df: DataFrame containing power data
        power_column: Name of the power value column
        unit_column: Name of the unit column
    
    Returns:
        DataFrame with normalized power values in Watts
    """
    df_normalized = df.copy()
    
    if unit_column in df.columns and power_column in df.columns:
        # Convert each row based on its unit
        df_normalized['power_w'] = df.apply(
            lambda row: convert_power_series_to_watts(
                pd.Series([row[power_column]]), 
                row[unit_column]
            ).iloc[0], 
            axis=1
        )
    else:
        # Assume all values are already in Watts
        df_normalized['power_w'] = df[power_column]
    
    return df_normalized
