#!/usr/bin/env python3
"""
Query multiple power metrics for a node and calculate total energy consumption.
"""

import sys
import os
import pandas as pd
from datetime import datetime
import argparse

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.compute.idrac import get_compute_metrics_with_joins
from database.connection_pool import get_pooled_connection
from analysis.energy import compute_energy_kwh_for_hostname


def query_metric_energy(hostname: str, metric_id: str, start_time: str, end_time: str, database: str = 'zen4') -> float:
    """
    Query a metric and calculate total energy consumption.
    
    Args:
        hostname: Hostname to query
        metric_id: Metric ID to query
        start_time: Start time string (YYYY-MM-DD HH:MM:SS)
        end_time: End time string (YYYY-MM-DD HH:MM:SS)
        database: Database name
    
    Returns:
        Energy consumption in kWh
    """
    try:
        # Query metric
        query = get_compute_metrics_with_joins(
            metric_id=metric_id,
            hostname=hostname,
            start_time=start_time,
            end_time=end_time
        )
        
        # Execute query
        with get_pooled_connection(database, 'idrac') as client:
            df = pd.read_sql_query(query, client.db_connection)
            
            if df.empty:
                return 0.0
            
            # Get unit from the data
            unit = df['units'].iloc[0] if 'units' in df.columns and df['units'].notna().any() else 'W'
            
            # Check for timezone issues
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            data_start = df['timestamp'].min()
            data_end = df['timestamp'].max()
            
            query_start_utc = pd.to_datetime(start_time, utc=True)
            query_end_utc = pd.to_datetime(end_time, utc=True)
            
            # Detect timezone mismatch
            use_boundaries = True
            if query_start_utc and query_end_utc and len(df) > 0:
                time_gap_before = (data_start - query_start_utc).total_seconds() if data_start > query_start_utc else 0
                time_gap_after = (query_end_utc - data_end).total_seconds() if query_end_utc > data_end else 0
                
                if time_gap_before > 7200 or time_gap_after > 7200:
                    use_boundaries = False
            
            # Calculate energy
            if use_boundaries:
                energy_kwh = compute_energy_kwh_for_hostname(
                    df, unit, hostname, start_time, end_time
                )
            else:
                # Use data range only (no boundary handling)
                energy_kwh = compute_energy_kwh_for_hostname(
                    df, unit, hostname, None, None
                )
            
            return energy_kwh
            
    except Exception as e:
        print(f"  ❌ Error querying {metric_id}: {e}")
        return 0.0


def query_node_energy(hostname: str, start_time: str, end_time: str, database: str = 'zen4'):
    """
    Query multiple metrics for a node and calculate total energy.
    
    Args:
        hostname: Hostname to query
        start_time: Start time string (YYYY-MM-DD HH:MM:SS)
        end_time: End time string (YYYY-MM-DD HH:MM:SS)
        database: Database name
    """
    print(f"📊 Querying energy consumption for node")
    print(f"🖥️  Hostname: {hostname}")
    print(f"📅 Time range: {start_time} to {end_time}")
    print(f"🗄️  Database: {database}")
    print()
    
    # Define metrics to query
    metrics = [
        'SystemPowerConsumption',
        'SystemOutputPower',
        'TotalCPUPower',
        'TotalStoragePower',
        'TotalMemoryPower',
        'TotalFanPower'
    ]
    
    print("⚡ Calculating energy for each metric...")
    print()
    
    results = {}
    total_energy = 0.0
    
    for metric in metrics:
        print(f"  Querying {metric}...", end=' ')
        energy = query_metric_energy(hostname, metric, start_time, end_time, database)
        results[metric] = energy
        total_energy += energy
        energy_wh = energy * 1000.0
        print(f"✓ {energy_wh:.6f} Wh")
    
    print()
    print("=" * 70)
    print("ENERGY CONSUMPTION SUMMARY")
    print("=" * 70)
    print()
    
    # Display results in Wh with high precision
    for metric, energy in results.items():
        energy_wh = energy * 1000.0
        print(f"{metric:30} {energy_wh:15.6f} Wh")
    
    print("-" * 70)
    total_energy_wh = total_energy * 1000.0
    print(f"{'TOTAL ENERGY':30} {total_energy_wh:15.6f} Wh")
    print()
    print("=" * 70)


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Query multiple power metrics for a node and calculate total energy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query node 62 (rpc-95-2) for a time range
  python query_node_energy.py --hostname rpc-95-2 --start-time "2025-12-06 17:38:13" --end-time "2025-12-06 17:48:31"
  
  # Query by nodeid (will resolve to hostname)
  python query_node_energy.py --nodeid 62 --start-time "2025-12-06 17:38:13" --end-time "2025-12-06 17:48:31"
        """
    )
    
    parser.add_argument('--hostname', help='Hostname to query (e.g., rpc-95-2)')
    parser.add_argument('--nodeid', type=int, help='Node ID to query (will resolve to hostname)')
    parser.add_argument('--start-time', required=True, help='Start time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-time', required=True, help='End time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--database', choices=['h100', 'zen4'], default='zen4', 
                       help='Database name (default: zen4)')
    
    args = parser.parse_args()
    
    # Resolve hostname from nodeid if provided
    hostname = args.hostname
    if args.nodeid and not hostname:
        # Query database to get hostname from nodeid
        try:
            with get_pooled_connection(args.database, 'public') as client:
                query = f"SELECT hostname FROM public.nodes WHERE nodeid = {args.nodeid}"
                df = pd.read_sql_query(query, client.db_connection)
                if not df.empty:
                    hostname = df['hostname'].iloc[0]
                    print(f"✓ Resolved nodeid {args.nodeid} to hostname: {hostname}")
                else:
                    print(f"❌ Node ID {args.nodeid} not found in database")
                    return
        except Exception as e:
            print(f"❌ Error resolving nodeid: {e}")
            return
    
    if not hostname:
        print("❌ Either --hostname or --nodeid must be provided")
        return
    
    # Query energy
    query_node_energy(hostname, args.start_time, args.end_time, args.database)


if __name__ == '__main__':
    main()

