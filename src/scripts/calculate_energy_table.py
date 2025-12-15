#!/usr/bin/env python3
"""
Script to calculate SystemPowerConsumption energy for each row in a table.
Reads a table with Start time and End time columns, queries SystemPowerConsumption
for hostname rpc-95-2 (nodeid 62), and fills in the energy values.
"""

import sys
import os
import pandas as pd
from datetime import datetime
import argparse
import pytz

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.compute.idrac import get_compute_metrics_with_joins
from database.connection_pool import get_pooled_connection
from analysis.energy import compute_energy_kwh_for_hostname


def calculate_energy_for_row(hostname: str, start_time: str, end_time: str, database: str = 'zen4') -> tuple:
    """
    Calculate SystemPowerConsumption energy for a given time range.
    
    Args:
        hostname: Hostname to query (e.g., 'rpc-95-2')
        start_time: Start time string (YYYY-MM-DD HH:MM:SS)
        end_time: End time string (YYYY-MM-DD HH:MM:SS)
        database: Database name ('zen4' for rpc nodes)
    
    Returns:
        Tuple of (energy consumption in kWh, raw DataFrame)
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
                print(f"⚠️  No data found for {hostname} from {start_time} to {end_time}")
                return 0.0, pd.DataFrame()
            
            # Get unit from the data
            unit = df['units'].iloc[0] if 'units' in df.columns and df['units'].notna().any() else 'W'
            
            # Check if we have data in the requested time range
            # The database returns UTC timestamps, so we need to compare properly
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            data_start = df['timestamp'].min()
            data_end = df['timestamp'].max()
            
            # Parse query times - try as UTC first, but detect timezone issues
            query_start_utc = pd.to_datetime(start_time, utc=True) if start_time else None
            query_end_utc = pd.to_datetime(end_time, utc=True) if end_time else None
            
            # Detect timezone mismatch: if query times are way before data times, likely timezone issue
            use_boundaries = True
            if query_start_utc and query_end_utc and len(df) > 0:
                # Calculate gaps
                time_gap_before = (data_start - query_start_utc).total_seconds() if data_start > query_start_utc else 0
                time_gap_after = (query_end_utc - data_end).total_seconds() if query_end_utc > data_end else 0
                
                # If there's a large gap (more than 1 hour) before the data, likely timezone issue
                # Also check if query time is way before data (more than 2 hours suggests timezone)
                if time_gap_before > 7200:  # 2 hours
                    use_boundaries = False
                    print(f"  ⚠️  Large time gap detected ({time_gap_before/3600:.1f}h before data, likely timezone issue), using data range only")
                elif time_gap_after > 7200:  # 2 hours
                    use_boundaries = False
                    print(f"  ⚠️  Large time gap detected ({time_gap_after/3600:.1f}h after data, likely timezone issue), using data range only")
                elif abs(time_gap_before) > 3600 or abs(time_gap_after) > 3600:
                    # Gap is significant but not huge - might be legitimate, but be cautious
                    # Check if query duration matches expected duration
                    query_duration = (query_end_utc - query_start_utc).total_seconds()
                    data_duration = (data_end - data_start).total_seconds()
                    
                    # If query is much shorter than data span, likely timezone issue
                    if query_duration > 0 and data_duration > query_duration * 2:
                        use_boundaries = False
                        print(f"  ⚠️  Query duration ({query_duration/60:.1f}min) much shorter than data span ({data_duration/60:.1f}min), using data range only")
            
            # Calculate energy
            if use_boundaries:
                energy_kwh = compute_energy_kwh_for_hostname(
                    df, 
                    unit, 
                    hostname, 
                    start_time, 
                    end_time
                )
            else:
                # Use data range only (no boundary handling) - this avoids timezone issues
                energy_kwh = compute_energy_kwh_for_hostname(
                    df, 
                    unit, 
                    hostname, 
                    None,  # No boundary start
                    None   # No boundary end
                )
            
            return energy_kwh, df.copy()
            
    except Exception as e:
        print(f"❌ Error calculating energy for {start_time} to {end_time}: {e}")
        return 0.0, pd.DataFrame()


def parse_time_column(time_str):
    """
    Parse time column which might be in various formats.
    Handles formats like:
    - "12/6/25 17:10"
    - "12/6/25 17:10:00"
    - "12/7/25 4:36"
    - "2025-12-06 17:10:00"
    
    Note: Times are assumed to be in UTC to match database timestamps.
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
                # Assume UTC timezone to match database
                import pytz
                dt_utc = pytz.UTC.localize(dt) if dt.tzinfo is None else dt.astimezone(pytz.UTC)
                return dt_utc.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        
        # If all formats fail, try pandas parsing
        dt = pd.to_datetime(time_str)
        # Ensure UTC
        if dt.tzinfo is None:
            import pytz
            dt = pytz.UTC.localize(dt)
        else:
            dt = dt.astimezone(pytz.UTC)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
        
    except Exception as e:
        print(f"⚠️  Could not parse time '{time_str}': {e}")
        return None


def process_table(input_file: str, output_file: str = None, hostname: str = 'rpc-95-2', database: str = 'zen4'):
    """
    Process the table and calculate SystemPowerConsumption energy for each row.
    
    Args:
        input_file: Path to input Excel or CSV file
        output_file: Path to output file (if None, adds '_filled' to input filename)
        hostname: Hostname to query (default: 'rpc-95-2')
        database: Database name (default: 'zen4')
    """
    print(f"📊 Processing table: {input_file}")
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
    print(f"  Columns: {', '.join(df.columns)}")
    print()
    
    # Check required columns
    required_cols = ['Start time', 'End time']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing required columns: {', '.join(missing_cols)}")
        print(f"   Available columns: {', '.join(df.columns)}")
        return
    
    # Initialize SystemPowerConsumption column if it doesn't exist
    if 'SystemPowerConsumption (kWh)' not in df.columns:
        df['SystemPowerConsumption (kWh)'] = 0.0
        print("✓ Added 'SystemPowerConsumption (kWh)' column")
    
    # Process each row
    print("⚡ Calculating energy for each row...")
    print()
    
    # Store raw data for each rank
    raw_data_by_rank = {}
    
    for idx, row in df.iterrows():
        rank = row.get('Rank', idx + 1)
        start_time_str = str(row['Start time'])
        end_time_str = str(row['End time'])
        
        # Parse times
        start_time = parse_time_column(start_time_str)
        end_time = parse_time_column(end_time_str)
        
        if not start_time or not end_time:
            print(f"⚠️  Row {idx + 1}: Could not parse times, skipping")
            continue
        
        # Calculate energy and get raw data
        print(f"  Row {idx + 1}/{len(df)}: {start_time} to {end_time}...", end=' ')
        energy_kwh, raw_df = calculate_energy_for_row(hostname, start_time, end_time, database)
        df.at[idx, 'SystemPowerConsumption (kWh)'] = energy_kwh
        print(f"✓ {energy_kwh:.4f} kWh")
        
        # Store raw data for this rank
        if not raw_df.empty:
            # Add metadata columns
            raw_df_copy = raw_df.copy()
            raw_df_copy.insert(0, 'Rank', rank)
            raw_df_copy.insert(1, 'Query_Start_Time', start_time)
            raw_df_copy.insert(2, 'Query_End_Time', end_time)
            raw_data_by_rank[f'Rank_{rank}'] = raw_df_copy
    
    print()
    print("✅ Energy calculation completed for all rows")
    print()
    
    # Save output file
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        ext = os.path.splitext(input_file)[1]
        output_file = f"{base_name}_filled{ext}"
    
    try:
        if output_file.endswith('.xlsx') or output_file.endswith('.xls'):
            df.to_excel(output_file, index=False)
        else:
            df.to_csv(output_file, index=False)
        
        print(f"💾 Saved results to: {output_file}")
        
        # Print summary
        print()
        print("📊 Summary:")
        print(f"  Total rows processed: {len(df)}")
        print(f"  Total energy: {df['SystemPowerConsumption (kWh)'].sum():.4f} kWh")
        print(f"  Average energy per row: {df['SystemPowerConsumption (kWh)'].mean():.4f} kWh")
        print(f"  Min energy: {df['SystemPowerConsumption (kWh)'].min():.4f} kWh")
        print(f"  Max energy: {df['SystemPowerConsumption (kWh)'].max():.4f} kWh")
        
    except Exception as e:
        print(f"❌ Error saving file: {e}")
    
    # Export raw data to separate Excel file
    if raw_data_by_rank:
        print()
        print("📥 Exporting raw power data...")
        
        # Create output filename for raw data
        base_name = os.path.splitext(input_file)[0]
        raw_output_file = f"{base_name}_raw_data.xlsx"
        
        try:
            with pd.ExcelWriter(raw_output_file, engine='openpyxl') as writer:
                for sheet_name, data_df in raw_data_by_rank.items():
                    # Clean sheet name (Excel has limitations)
                    clean_sheet_name = sheet_name.replace('/', '_').replace('\\', '_')[:31]
                    
                    # Remove timezone info from timestamps for Excel compatibility
                    if 'timestamp' in data_df.columns:
                        data_df_copy = data_df.copy()
                        # Handle timezone-aware timestamps
                        if data_df_copy['timestamp'].dtype.name.startswith('datetime64'):
                            if hasattr(data_df_copy['timestamp'].iloc[0], 'tz') and data_df_copy['timestamp'].iloc[0].tz is not None:
                                data_df_copy['timestamp'] = data_df_copy['timestamp'].dt.tz_localize(None)
                        data_df_copy.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                    else:
                        data_df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                    
                    print(f"  ✓ {clean_sheet_name}: {len(data_df)} rows")
            
            print()
            print(f"💾 Saved raw data to: {raw_output_file}")
            print(f"   Total sheets: {len(raw_data_by_rank)}")
            print(f"   Total data points: {sum(len(df) for df in raw_data_by_rank.values())}")
            
        except Exception as e:
            print(f"❌ Error saving raw data file: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Calculate SystemPowerConsumption energy for each row in a table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process Excel file with default settings (rpc-95-2, zen4)
  python calculate_energy_table.py table.xlsx
  
  # Process CSV file with custom output
  python calculate_energy_table.py table.csv -o output.csv
  
  # Process with different hostname
  python calculate_energy_table.py table.xlsx --hostname rpc-95-1
        """
    )
    
    parser.add_argument('input_file', help='Input Excel or CSV file with Start time and End time columns')
    parser.add_argument('-o', '--output', help='Output file path (default: adds _filled to input filename)')
    parser.add_argument('--hostname', default='rpc-95-2', help='Hostname to query (default: rpc-95-2)')
    parser.add_argument('--database', choices=['h100', 'zen4'], default='zen4', 
                       help='Database name (default: zen4)')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        return
    
    # Process table
    process_table(args.input_file, args.output, args.hostname, args.database)


if __name__ == '__main__':
    main()

