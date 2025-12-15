#!/usr/bin/env python3
"""
Script to dump raw SystemPowerConsumption data for a specific time period.
This helps debug energy calculation issues by showing the actual power values.
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
from utils.conversions import convert_power_series_to_watts


def dump_raw_data(hostname: str, start_time: str, end_time: str, database: str = 'zen4', output_file: str = None):
    """
    Dump raw SystemPowerConsumption data for a time period.
    
    Args:
        hostname: Hostname to query (e.g., 'rpc-95-2')
        start_time: Start time string (YYYY-MM-DD HH:MM:SS)
        end_time: End time string (YYYY-MM-DD HH:MM:SS)
        database: Database name ('zen4' for rpc nodes)
        output_file: Optional output CSV file
    """
    print(f"📊 Querying raw SystemPowerConsumption data")
    print(f"🖥️  Hostname: {hostname}")
    print(f"📅 Time range: {start_time} to {end_time}")
    print(f"🗄️  Database: {database}")
    print()
    
    try:
        # Query SystemPowerConsumption metric
        query = get_compute_metrics_with_joins(
            metric_id='SystemPowerConsumption',
            hostname=hostname,
            start_time=start_time,
            end_time=end_time
        )
        
        print("🔍 SQL Query:")
        print(query)
        print()
        
        # Execute query
        with get_pooled_connection(database, 'idrac') as client:
            df = pd.read_sql_query(query, client.db_connection)
            
            if df.empty:
                print(f"⚠️  No data found for {hostname} from {start_time} to {end_time}")
                return
            
            print(f"✓ Retrieved {len(df)} data points")
            print()
            
            # Get unit information
            unit = df['units'].iloc[0] if 'units' in df.columns and df['units'].notna().any() else 'W'
            print(f"📏 Unit from database: {unit}")
            print()
            
            # Show raw data
            print("=" * 100)
            print("RAW DATA (First 20 rows):")
            print("=" * 100)
            
            display_cols = ['timestamp', 'hostname', 'value', 'units', 'source', 'fqdd']
            available_cols = [col for col in display_cols if col in df.columns]
            
            print(df[available_cols].head(20).to_string(index=False))
            print()
            
            if len(df) > 20:
                print(f"... ({len(df) - 20} more rows)")
                print()
            
            # Statistics
            print("=" * 100)
            print("STATISTICS:")
            print("=" * 100)
            print(f"Total data points: {len(df)}")
            print(f"Time range in data: {df['timestamp'].min()} to {df['timestamp'].max()}")
            print(f"Unit: {unit}")
            print()
            print("Raw value statistics:")
            print(f"  Min: {df['value'].min():.2f} {unit}")
            print(f"  Max: {df['value'].max():.2f} {unit}")
            print(f"  Mean: {df['value'].mean():.2f} {unit}")
            print(f"  Median: {df['value'].median():.2f} {unit}")
            print()
            
            # Convert to Watts and show
            if 'value' in df.columns:
                df['power_w'] = convert_power_series_to_watts(df['value'], unit)
                print("Converted to Watts:")
                print(f"  Min: {df['power_w'].min():.2f} W")
                print(f"  Max: {df['power_w'].max():.2f} W")
                print(f"  Mean: {df['power_w'].mean():.2f} W")
                print(f"  Median: {df['power_w'].median():.2f} W")
                print()
                
                # Calculate time differences
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')
                df['time_diff_seconds'] = df['timestamp'].diff().dt.total_seconds()
                
                # Show time intervals
                print("Time intervals (first 10):")
                interval_df = df[['timestamp', 'power_w', 'time_diff_seconds']].head(10).copy()
                print(interval_df.to_string(index=False))
                print()
                
                # Calculate energy manually for verification
                total_time_seconds = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
                avg_power_w = df['power_w'].mean()
                simple_energy_kwh = (avg_power_w * total_time_seconds) / 3_600_000.0
                
                print("=" * 100)
                print("ENERGY ESTIMATION:")
                print("=" * 100)
                print(f"Total time span: {total_time_seconds:.1f} seconds ({total_time_seconds/3600:.4f} hours)")
                print(f"Average power: {avg_power_w:.2f} W")
                print(f"Simple estimation (avg_power * time): {simple_energy_kwh:.4f} kWh")
                print()
            
            # Save to CSV if requested
            if output_file:
                df.to_csv(output_file, index=False)
                print(f"💾 Saved full data to: {output_file}")
                print(f"   Total rows: {len(df)}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Dump raw SystemPowerConsumption data for debugging',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dump data for a specific time range
  python dump_raw_power_data.py --hostname rpc-95-2 --start-time "2025-12-06 17:10:00" --end-time "2025-12-06 17:12:00"
  
  # Save to CSV file
  python dump_raw_power_data.py --hostname rpc-95-2 --start-time "2025-12-06 17:10:00" --end-time "2025-12-06 17:12:00" -o raw_data.csv
        """
    )
    
    parser.add_argument('--hostname', default='rpc-95-2', help='Hostname to query (default: rpc-95-2)')
    parser.add_argument('--start-time', required=True, help='Start time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-time', required=True, help='End time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--database', choices=['h100', 'zen4'], default='zen4', 
                       help='Database name (default: zen4)')
    parser.add_argument('-o', '--output', help='Output CSV file path')
    
    args = parser.parse_args()
    
    dump_raw_data(args.hostname, args.start_time, args.end_time, args.database, args.output)


if __name__ == '__main__':
    main()

