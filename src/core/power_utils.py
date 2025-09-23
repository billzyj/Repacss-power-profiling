#!/usr/bin/env python3
"""
Power utility functions for REPACSS Power Measurement
Provides common functions for handling power data conversions and corrections
"""

from typing import List, Dict
import pandas as pd
from queries.compute.idrac import get_compute_metrics_with_joins
from queries.infra.irc_pdu import get_irc_metrics_with_joins, get_pdu_metrics_with_joins
from core.database import get_raw_database_connection

# Metrics to exclude from graphs (not power consumption)
EXCLUDED_METRICS = [
    'systemheadroominstantaneous'  # This is remaining wattage, not consumption
]

# Derived metrics that are not realpower consumption
DERIVED_METRICS = [
    'computepower',              # Compute power that is not wasted
    'systemheadroominstantaneous'  # This is remaining wattage, not consumption
]

IRC_NODES = ['irc-91-5', 'irc-92-5', 'irc-93-3', 'irc-94-5', 'irc-95-3', 'irc-96-5']
IRC_POWER_METRICS = ['CompressorPower', 'CondenserFanPower', 'CoolDemand', 'CoolOutput', 'TotalAirSideCoolingDemand', 'TotalSensibleCoolingPower']

PDU_NODES = ['pdu-91-1','pdu-91-2','pdu-91-3','pdu-91-4','pdu-92-1','pdu-92-2','pdu-92-3','pdu-92-4','pdu-93-1','pdu-93-2',
             'pdu-94-1','pdu-94-2','pdu-94-3','pdu-94-4','pdu-95-1','pdu-95-2','pdu-96-1','pdu-96-2','pdu-96-3','pdu-96-4','pdu-97-1','pdu-97-2','pdu-97-3','pdu-97-4']
PDU_POWER_METRICS = ['pdu']

RACK_91_COMPUTE_NODES = ['rpc-91-1', 'rpc-91-2', 'rpc-91-3', 'rpc-91-4', 'rpc-91-5', 'rpc-91-6', 'rpc-91-7', 'rpc-91-8', 'rpc-91-9', 'rpc-91-10',
                        'rpc-91-11', 'rpc-91-12', 'rpc-91-13', 'rpc-91-14', 'rpc-91-15', 'rpc-91-16', 'rpc-91-17', 'rpc-91-18', 'rpc-91-19', 'rpc-91-20']
                        # One more infiniband switch and ethernet switch
RACK_91_PD_NODES = ['pdu-91-1', 'pdu-91-2', 'pdu-91-3', 'pdu-91-4']

RACK_92_COMPUTE_NODES = ['rpc-92-1', 'rpc-92-2', 'rpc-92-3', 'rpc-92-4', 'rpc-92-5', 'rpc-92-6', 'rpc-92-7', 'rpc-92-8', 'rpc-92-9', 'rpc-92-10',
                        'rpc-92-11', 'rpc-92-12', 'rpc-92-13', 'rpc-92-14', 'rpc-92-15', 'rpc-92-16', 'rpc-92-17', 'rpc-92-18', 'rpc-92-19', 'rpc-92-20']
                        # Two more AMD test nodes
RACK_92_PD_NODES = ['pdu-92-1', 'pdu-92-2', 'pdu-92-3', 'pdu-92-4']

RACK_93_COMPUTE_NODES = ['rpg-93-1', 'rpg-93-2', 'rpg-93-3', 'rpg-93-4', 'rpg-93-5', 'rpg-93-6', 'rpg-93-7', 'rpg-93-8']
RACK_93_PD_NODES = ['pdu-93-1', 'pdu-93-2']
                        # One more ethernet switch, one ttu switch, one 1GB switch, one GPU-build node, one login node, two head nodes, one monitor node, one globus node

RACK_94_COMPUTE_NODES = ['rpc-94-1', 'rpc-94-2', 'rpc-94-3', 'rpc-94-4', 'rpc-94-5', 'rpc-94-6', 'rpc-94-7', 'rpc-94-8', 'rpc-94-9', 'rpc-94-10',
                        'rpc-94-11', 'rpc-94-12', 'rpc-94-13', 'rpc-94-14', 'rpc-94-15', 'rpc-94-16', 'rpc-94-17', 'rpc-94-18', 'rpc-94-19', 'rpc-94-20']
                        # One more infiniband switch and ethernet switch
RACK_94_PD_NODES = ['pdu-94-1', 'pdu-94-2', 'pdu-94-3', 'pdu-94-4']

RACK_95_COMPUTE_NODES = ['rpc-95-1', 'rpc-95-2', 'rpc-95-3', 'rpc-95-4', 'rpc-95-5', 'rpc-95-6', 'rpc-95-7', 'rpc-95-8', 'rpc-95-9', 'rpc-95-10']
RACK_95_PD_NODES = ['pdu-95-1', 'pdu-95-2']
                        # One ethernet switch, two infiniband switches, 9 hammerspace nodes not reported.
RACK_96_COMPUTE_NODES = ['rpc-96-1', 'rpc-96-2', 'rpc-96-3', 'rpc-96-4', 'rpc-96-5', 'rpc-96-6', 'rpc-96-7', 'rpc-96-8', 'rpc-96-9', 'rpc-96-10',
                        'rpc-96-11', 'rpc-96-12', 'rpc-96-13', 'rpc-96-14', 'rpc-96-15', 'rpc-96-16', 'rpc-96-17', 'rpc-96-18', 'rpc-96-19', 'rpc-96-20']
RACK_96_PD_NODES = ['pdu-96-1', 'pdu-96-2', 'pdu-96-3', 'pdu-96-4']
                       # One more infiniband switch and ethernet switch 
RACK_97_COMPUTE_NODES = ['rpc-97-1', 'rpc-97-2', 'rpc-97-3', 'rpc-97-4', 'rpc-97-5', 'rpc-97-6', 'rpc-97-7', 'rpc-97-8', 'rpc-97-9', 'rpc-97-10',
                        'rpc-97-11', 'rpc-97-12', 'rpc-97-13', 'rpc-97-14', 'rpc-97-15', 'rpc-97-16', 'rpc-97-17', 'rpc-97-18', 'rpc-97-19', 'rpc-97-20']
RACK_97_PDU_NODES = ['pdu-97-1', 'pdu-97-2', 'pdu-97-3', 'pdu-97-4']


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
    Check if a metric should be excluded from graphs
    
    Args:
        metric_name: The metric name (lowercase)
    
    Returns:
        True if metric should be excluded
    """
    return metric_name.lower() in EXCLUDED_METRICS

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

def _convert_power_series_to_watts(power_series: "pd.Series", unit: str) -> "pd.Series":
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

def compute_energy_kwh_for_hostname(df, unit: str, hostname: str, start_time: str = None, end_time: str = None) -> float:
    """
    Compute energy consumption (kWh) for a given hostname over the DataFrame's time range.
    
    Handles edge cases where query results don't include exact start/end times by
    using the first/last power values to estimate energy for missing boundary periods.

    The DataFrame is expected to contain columns: 'timestamp', 'hostname', 'value'.
    Power values will be converted to Watts before integration based on the unit.

    Integration uses trapezoidal rule over irregular sampling.

    Args:
        df: Query result DataFrame with at least 'timestamp', 'hostname', 'value'
        unit: Unit from metrics_definition table (e.g., 'mW', 'kW', 'W')
        hostname: Hostname to filter by
        start_time: Original query start time (optional, for boundary estimation)
        end_time: Original query end time (optional, for boundary estimation)

    Returns:
        Energy in kWh as a float (0.0 if insufficient data)
    """
    required_cols = {"timestamp", "hostname", "value"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing required columns: {sorted(missing)}")

    host_df = df[df["hostname"] == hostname].copy()
    if host_df.empty:
        return 0.0

    # Parse and sort by timestamp
    host_df["timestamp"] = pd.to_datetime(host_df["timestamp"], utc=True, errors="coerce")
    host_df = host_df.dropna(subset=["timestamp"]).sort_values("timestamp")
    if len(host_df) < 1:
        return 0.0

    # Convert power to Watts
    host_df["power_w"] = _convert_power_series_to_watts(host_df["value"], unit)

    # Handle edge cases for boundary energy estimation
    total_energy_joules = 0.0
    
    # Parse original query times if provided
    query_start = None
    query_end = None
    if start_time:
        query_start = pd.to_datetime(start_time, utc=True)
    if end_time:
        query_end = pd.to_datetime(end_time, utc=True)
    
    # Get actual data boundaries
    data_start = host_df["timestamp"].iloc[0]
    data_end = host_df["timestamp"].iloc[-1]
    first_power = host_df["power_w"].iloc[0]
    last_power = host_df["power_w"].iloc[-1]
    
    # Estimate energy for missing start period (query_start to data_start)
    if query_start and data_start > query_start:
        start_duration = (data_start - query_start).total_seconds()
        start_energy = first_power * start_duration
        total_energy_joules += start_energy
    
    # Calculate energy for the actual data period using trapezoidal rule
    if len(host_df) >= 2:
        time_seconds = host_df["timestamp"].astype("int64") // 10**9
        dt = time_seconds.diff().astype("float64")  # seconds between samples
        avg_power = (host_df["power_w"] + host_df["power_w"].shift(1)) / 2.0
        data_energy = (avg_power * dt).sum(skipna=True)
        total_energy_joules += data_energy
    else:
        # Single data point - use it for the entire period if we have query boundaries
        if query_start and query_end:
            total_duration = (query_end - query_start).total_seconds()
            total_energy_joules = first_power * total_duration
        else:
            # No query boundaries, can't estimate energy accurately
            return 0.0
    
    # Estimate energy for missing end period (data_end to query_end)
    if query_end and data_end < query_end:
        end_duration = (query_end - data_end).total_seconds()
        end_energy = last_power * end_duration
        total_energy_joules += end_energy

    # Convert Joules to kWh (1 kWh = 3.6e6 J)
    energy_kwh = float(total_energy_joules) / 3_600_000.0
    return max(energy_kwh, 0.0)



def get_node_type_and_query_func(hostname: str):
    """
    Determine node type and return appropriate query function.
    
    Returns:
        tuple: (node_type, query_function, database, schema)
    """
    hostname_mapping = {
        'pdu': ('pdu', get_pdu_metrics_with_joins, 'infra', 'pdu'),
        'irc': ('irc', get_irc_metrics_with_joins, 'infra', 'irc'),
        'rpg': ('h100', get_compute_metrics_with_joins, 'h100', 'idrac'),
        'rpc': ('zen4', get_compute_metrics_with_joins, 'zen4', 'idrac')
    }
    
    for prefix, (node_type, query_func, database, schema) in hostname_mapping.items():
        if hostname.startswith(prefix):
            return node_type, query_func, database, schema
    
    raise ValueError(f"Invalid hostname: {hostname}")

def get_compute_power_metrics(database: str, schema: str) -> List[str]:
    """
    Get power metrics for compute nodes from the database.
    
    Args:
        database: Database name ('h100' or 'zen4')
        schema: Schema name ('idrac')
    
    Returns:
        List of metric IDs that are power-related
    """
    from queries.compute.public import POWER_METRICS_QUERY_UNIT_IN_MW_W_KW
    
    db_connection = get_raw_database_connection(database, schema)
    try:
        df = pd.read_sql_query(POWER_METRICS_QUERY_UNIT_IN_MW_W_KW, db_connection)
        return df['metric_id'].tolist()
    finally:
        if db_connection:
            db_connection.close()

def get_power_metrics_with_joins(metrics: List[str], hostname: str, start_time: str, end_time: str):
    """
    Generate queries for multiple metrics for a given hostname.
    Returns dictionary with metric_id as key and (query, energy) as value.
    """
    node_type, query_func, database, schema = get_node_type_and_query_func(hostname)
    
    queries = {}
    for metric in metrics:
        if node_type in ['pdu']:
            # PDU doesn't use metric_id parameter
            query = query_func(hostname, start_time, end_time)
        else:
            # IRC and compute nodes use metric_id
            query = query_func(metric, hostname, start_time, end_time)
        
        # Note: energy calculation requires actual data, not just query string
        queries[metric] = query
    
    return queries, database, schema

def power_analysis(hostname: str, start_time: str, end_time: str, metrics: List[str] = None):
    """
    Single node power analysis for a given hostname and time range.
    
    Args:
        hostname: Node hostname
        start_time: Start timestamp
        end_time: End timestamp  
        metrics: List of metrics to analyze (if None, uses all available)
    
    Returns:
        DataFrame with power data and energy calculations
    """
    if metrics is None:
        # Default metrics based on node type
        node_type, _, database, schema = get_node_type_and_query_func(hostname)
        if node_type == 'pdu':
            metrics = PDU_POWER_METRICS
        elif node_type == 'irc':
            metrics = IRC_POWER_METRICS
        else:  # compute nodes - get from database
            metrics = get_compute_power_metrics(database, schema)
    
    queries, database, schema = get_power_metrics_with_joins(metrics, hostname, start_time, end_time)
    
    # Connect to database
    db_connection = get_raw_database_connection(database, schema)
    
    try:
        # Execute queries and combine results
        all_data = []
        for metric, query in queries.items():
            df = pd.read_sql_query(query, db_connection)
            if not df.empty:
                df['metric'] = metric
                all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Calculate energy for each metric and add to DataFrame
        energy_results = {}
        for metric in metrics:
            metric_data = combined_df[combined_df['metric'] == metric].copy()
            if not metric_data.empty:
                # Get unit from the data (assuming units column exists)
                unit = metric_data['units'].iloc[0] if 'units' in metric_data.columns else 'W'
                
                # Calculate total energy for this metric using the dedicated function
                total_energy_kwh = compute_energy_kwh_for_hostname(metric_data, unit, hostname, start_time, end_time)
                energy_results[metric] = total_energy_kwh
                
                # Sort by timestamp and add power conversion
                metric_data = metric_data.sort_values('timestamp')
                metric_data['power_w'] = _convert_power_series_to_watts(metric_data['value'], unit)
                
                # Add energy calculation columns for display
                metric_data['time_diff_seconds'] = metric_data['timestamp'].diff().dt.total_seconds()
                metric_data['avg_power_w'] = (metric_data['power_w'] + metric_data['power_w'].shift(1)) / 2.0
                metric_data['energy_interval_kwh'] = (metric_data['avg_power_w'] * metric_data['time_diff_seconds']) / 3_600_000.0
                
                # Handle first row: set NaN for time-based calculations, 0 for cumulative
                if len(metric_data) > 0:
                    metric_data.loc[0, 'time_diff_seconds'] = pd.NA
                    metric_data.loc[0, 'avg_power_w'] = pd.NA
                    metric_data.loc[0, 'energy_interval_kwh'] = pd.NA
                
                # Calculate cumulative energy: first row = 0, then accumulate
                metric_data['cumulative_energy_kwh'] = 0.0
                if len(metric_data) > 0:
                    metric_data.loc[0, 'cumulative_energy_kwh'] = 0.0
                
                # Calculate cumulative energy for remaining rows
                for i in range(1, len(metric_data)):
                    if pd.notna(metric_data['time_diff_seconds'].iloc[i]) and pd.notna(metric_data['energy_interval_kwh'].iloc[i]):
                        metric_data.loc[i, 'cumulative_energy_kwh'] = (
                            metric_data['cumulative_energy_kwh'].iloc[i-1] + 
                            metric_data['energy_interval_kwh'].iloc[i]
                        )
                
                # Update the original combined_df with energy calculations
                original_indices = combined_df[combined_df['metric'] == metric].index
                for i, orig_idx in enumerate(original_indices):
                    if i < len(metric_data):
                        combined_df.loc[orig_idx, 'power_w'] = metric_data['power_w'].iloc[i]
                        combined_df.loc[orig_idx, 'time_diff_seconds'] = metric_data['time_diff_seconds'].iloc[i]
                        combined_df.loc[orig_idx, 'avg_power_w'] = metric_data['avg_power_w'].iloc[i]
                        combined_df.loc[orig_idx, 'energy_interval_kwh'] = metric_data['energy_interval_kwh'].iloc[i]
                        combined_df.loc[orig_idx, 'cumulative_energy_kwh'] = metric_data['cumulative_energy_kwh'].iloc[i]
        
        return combined_df, energy_results
        
    finally:
        if db_connection:
            db_connection.close()

def multi_node_power_analysis(hostnames: List[str], start_time: str, end_time: str, metrics: List[str] = None):
    """
    Multi-node power analysis with connection pooling for efficiency.
    
    Args:
        hostnames: List of node hostnames
        start_time: Start timestamp
        end_time: End timestamp
        metrics: List of metrics to analyze (if None, uses defaults per node type)
    
    Returns:
        Dictionary with hostname as key and (dataframe, energy_dict) as value
    """
    # Group hostnames by database/schema to reuse connections
    db_groups = {}
    for hostname in hostnames:
        _, _, database, schema = get_node_type_and_query_func(hostname)
        key = (database, schema)
        if key not in db_groups:
            db_groups[key] = []
        db_groups[key].append(hostname)
    
    results = {}
    
    # Process each database group
    for (database, schema), group_hostnames in db_groups.items():
        db_connection = get_raw_database_connection(database, schema)
        
        try:
            for hostname in group_hostnames:
                if metrics is None:
                    # Default metrics based on node type
                    node_type, _, host_database, host_schema = get_node_type_and_query_func(hostname)
                    if node_type == 'pdu':
                        host_metrics = PDU_POWER_METRICS
                    elif node_type == 'irc':
                        host_metrics = IRC_POWER_METRICS
                    else:  # compute nodes - get from database
                        host_metrics = get_compute_power_metrics(host_database, host_schema)
                else:
                    host_metrics = metrics
                
                queries, _, _ = get_power_metrics_with_joins(host_metrics, hostname, start_time, end_time)
                
                # Execute queries and combine results
                all_data = []
                for metric, query in queries.items():
                    df = pd.read_sql_query(query, db_connection)
                    if not df.empty:
                        df['metric'] = metric
                        all_data.append(df)
                
                if all_data:
                    combined_df = pd.concat(all_data, ignore_index=True)
                    
                    # Calculate energy for each metric and add to DataFrame
                    energy_results = {}
                    for metric in host_metrics:
                        metric_data = combined_df[combined_df['metric'] == metric].copy()
                        if not metric_data.empty:
                            # Get unit from the data (assuming units column exists)
                            unit = metric_data['units'].iloc[0] if 'units' in metric_data.columns else 'W'
                            
                            # Calculate total energy for this metric using the dedicated function
                            total_energy_kwh = compute_energy_kwh_for_hostname(metric_data, unit, hostname, start_time, end_time)
                            energy_results[metric] = total_energy_kwh
                            
                            # Sort by timestamp and add power conversion
                            metric_data = metric_data.sort_values('timestamp')
                            metric_data['power_w'] = _convert_power_series_to_watts(metric_data['value'], unit)
                            
                            # Add energy calculation columns for display
                            metric_data['time_diff_seconds'] = metric_data['timestamp'].diff().dt.total_seconds()
                            metric_data['avg_power_w'] = (metric_data['power_w'] + metric_data['power_w'].shift(1)) / 2.0
                            metric_data['energy_interval_kwh'] = (metric_data['avg_power_w'] * metric_data['time_diff_seconds']) / 3_600_000.0
                            
                            # Handle first row: set NaN for time-based calculations, 0 for cumulative
                            if len(metric_data) > 0:
                                metric_data.loc[0, 'time_diff_seconds'] = pd.NA
                                metric_data.loc[0, 'avg_power_w'] = pd.NA
                                metric_data.loc[0, 'energy_interval_kwh'] = pd.NA
                            
                            # Calculate cumulative energy: first row = 0, then accumulate
                            metric_data['cumulative_energy_kwh'] = 0.0
                            if len(metric_data) > 0:
                                metric_data.loc[0, 'cumulative_energy_kwh'] = 0.0
                            
                            # Calculate cumulative energy for remaining rows
                            for i in range(1, len(metric_data)):
                                if pd.notna(metric_data['time_diff_seconds'].iloc[i]) and pd.notna(metric_data['energy_interval_kwh'].iloc[i]):
                                    metric_data.loc[i, 'cumulative_energy_kwh'] = (
                                        metric_data['cumulative_energy_kwh'].iloc[i-1] + 
                                        metric_data['energy_interval_kwh'].iloc[i]
                                    )
                            
                            # Update the original combined_df with energy calculations
                            original_indices = combined_df[combined_df['metric'] == metric].index
                            for i, orig_idx in enumerate(original_indices):
                                if i < len(metric_data):
                                    combined_df.loc[orig_idx, 'power_w'] = metric_data['power_w'].iloc[i]
                                    combined_df.loc[orig_idx, 'time_diff_seconds'] = metric_data['time_diff_seconds'].iloc[i]
                                    combined_df.loc[orig_idx, 'avg_power_w'] = metric_data['avg_power_w'].iloc[i]
                                    combined_df.loc[orig_idx, 'energy_interval_kwh'] = metric_data['energy_interval_kwh'].iloc[i]
                                    combined_df.loc[orig_idx, 'cumulative_energy_kwh'] = metric_data['cumulative_energy_kwh'].iloc[i]
                    
                    results[hostname] = (combined_df, energy_results)
                else:
                    results[hostname] = (pd.DataFrame(), {})
                    
        finally:
            if db_connection:
                db_connection.close()
    
    return results
