#!/usr/bin/env python3
"""
Script to export raw SystemPowerConsumption data for each rank to Excel.
Each rank gets its own sheet with the raw query results.
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


def parse_time_column(time_str):
    """
    Parse time column which might be in various formats.
    Handles formats like:
    - "12/6/25 17:10"
    - "12/6/25 17:10:00"
    - "12/7/25 4:36"
    - "2025-12-06 17:10:00"
    """
    try:
        # Try common formats
        formats = [
            '%m/%d/%y %H:%M:%S',
            '%m/%d/%y %H:%M',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M',
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(time_str.strip(), fmt)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        
        # If all formats fail, try pandas parsing
        dt = pd.to_datetime(time_str)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
        
    except Exception as e:
        print(f"⚠️  Could not parse time '{time_str}': {e}")
        return None


def query_raw_power_data(hostname: str, start_time: str, end_time: str, database: str = 'zen4') -> pd.DataFrame:
    """
    Query raw SystemPowerConsumption data for a given time range.
    
    Args:
        hostname: Hostname to query (e.g., 'rpc-95-2')
        start_time: Start time string (YYYY-MM-DD HH:MM:SS)
        end_time: End time string (YYYY-MM-DD HH:MM:SS)
        database: Database name ('zen4' for rpc nodes)
    
    Returns:
        DataFrame with raw power data
    """
    try:
        # Query SystemPowerConsumption metric
        query = get_compute_metrics_with_joins(
            metric_id='SystemPowerConsumption',
            hostname=hostname,
            start_time=start_time,
            end_time=end_time
        )
        
        # Execute query
        with get_pooled_connection(database, 'idrac') as client:
            df = pd.read_sql_query(query, client.db_connection)
            
            if df.empty:
                print(f"  ⚠️  No data found")
                return pd.DataFrame()
            
            # Ensure timestamp is datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return df
            
    except Exception as e:
        print(f"  ❌ Error querying data: {e}")
        return pd.DataFrame()


def export_raw_data_to_excel(input_file: str, output_file: str = None, hostname: str = 'rpc-95-2', database: str = 'zen4'):
    """
    Export raw SystemPowerConsumption data for each rank to Excel.
    
    Args:
        input_file: Path to input Excel file with Rank, Start time, End time columns
        output_file: Path to output Excel file (if None, adds '_raw_data' to input filename)
        hostname: Hostname to query (default: 'rpc-95-2')
        database: Database name (default: 'zen4')
    """
    print(f"📊 Exporting raw power data from: {input_file}")
    print(f"🖥️  Hostname: {hostname}")
    print(f"🗄️  Database: {database}")
    print()
    
    # Read input file
    try:
        if input_file.endswith('.xlsx') or input_file.endswith('.xls'):
            df = pd.read_excel(input_file)
        else:
            df = pd.read_csv(input_file)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    print(f"✓ Read {len(df)} rows from {input_file}")
    print()
    
    # Check required columns
    required_cols = ['Rank', 'Start time', 'End time']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing required columns: {', '.join(missing_cols)}")
        return
    
    # Set output file
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_raw_data.xlsx"
    
    # Process each rank
    print("📥 Querying raw data for each rank...")
    print()
    
    all_data = {}
    
    for idx, row in df.iterrows():
        rank = row['Rank']
        start_time_str = str(row['Start time'])
        end_time_str = str(row['End time'])
        
        # Parse times
        start_time = parse_time_column(start_time_str)
        end_time = parse_time_column(end_time_str)
        
        if not start_time or not end_time:
            print(f"  Rank {rank}: ⚠️  Could not parse times, skipping")
            continue
        
        print(f"  Rank {rank}: {start_time} to {end_time}...", end=' ')
        
        # Query raw data
        raw_df = query_raw_power_data(hostname, start_time, end_time, database)
        
        if not raw_df.empty:
            # Add metadata columns
            raw_df.insert(0, 'Rank', rank)
            raw_df.insert(1, 'Query_Start_Time', start_time)
            raw_df.insert(2, 'Query_End_Time', end_time)
            
            # Calculate some statistics
            if 'value' in raw_df.columns:
                raw_df['power_w'] = raw_df['value']  # Assuming already in Watts based on units
                if 'units' in raw_df.columns:
                    unit = raw_df['units'].iloc[0] if raw_df['units'].notna().any() else 'W'
                    if unit.lower() == 'mw':
                        raw_df['power_w'] = raw_df['value'] / 1000.0
                    elif unit.lower() == 'kw':
                        raw_df['power_w'] = raw_df['value'] * 1000.0
            
            all_data[f'Rank_{rank}'] = raw_df
            print(f"✓ {len(raw_df)} data points")
        else:
            print(f"⚠️  No data")
            # Create empty DataFrame with same structure
            empty_df = pd.DataFrame(columns=['Rank', 'Query_Start_Time', 'Query_End_Time', 
                                           'timestamp', 'hostname', 'value', 'units', 'source', 'fqdd'])
            empty_df.loc[0] = [rank, start_time, end_time, None, hostname, None, None, None, None]
            all_data[f'Rank_{rank}'] = empty_df
    
    print()
    print("💾 Saving to Excel...")
    
    # Save to Excel with one sheet per rank
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet_name, data_df in all_data.items():
                # Clean sheet name (Excel has limitations)
                clean_sheet_name = sheet_name.replace('/', '_').replace('\\', '_')[:31]
                
                # Remove timezone info from timestamps for Excel compatibility
                if 'timestamp' in data_df.columns:
                    data_df_copy = data_df.copy()
                    data_df_copy['timestamp'] = data_df_copy['timestamp'].dt.tz_localize(None) if data_df_copy['timestamp'].dtype.name == 'datetime64[ns, UTC]' else data_df_copy['timestamp']
                    data_df_copy.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                else:
                    data_df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                
                print(f"  ✓ {clean_sheet_name}: {len(data_df)} rows")
        
        print()
        print(f"✅ Saved raw data to: {output_file}")
        print(f"   Total sheets: {len(all_data)}")
        print(f"   Total data points: {sum(len(df) for df in all_data.values())}")
        
    except Exception as e:
        print(f"❌ Error saving Excel file: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Export raw SystemPowerConsumption data for each rank to Excel',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export raw data with default settings (rpc-95-2, zen4)
  python export_raw_power_data.py table.xlsx
  
  # Specify output file
  python export_raw_power_data.py table.xlsx -o raw_power_data.xlsx
  
  # Process with different hostname
  python export_raw_power_data.py table.xlsx --hostname rpc-95-1
        """
    )
    
    parser.add_argument('input_file', help='Input Excel or CSV file with Rank, Start time, End time columns')
    parser.add_argument('-o', '--output', help='Output Excel file path (default: adds _raw_data to input filename)')
    parser.add_argument('--hostname', default='rpc-95-2', help='Hostname to query (default: rpc-95-2)')
    parser.add_argument('--database', choices=['h100', 'zen4'], default='zen4', 
                       help='Database name (default: zen4)')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        return
    
    # Export raw data
    export_raw_data_to_excel(args.input_file, args.output, args.hostname, args.database)


if __name__ == '__main__':
    main()

