#!/usr/bin/env python3
"""
Run ZEN4 Power Queries
Handles zen4.idrac schema queries for ZEN4 compute node power monitoring
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.compute.idrac import *
from core.database import (
    connect_to_database,
    disconnect_all
)
from core.power_utils import create_power_query_with_conversion


class Zen4PowerQueryRunner:
    """Manages ZEN4 power queries and analysis"""
    
    def __init__(self):
        self.client = None
        
    def connect_to_zen4(self):
        """Connect to ZEN4 database"""
        print("🔌 Connecting to ZEN4 database...")
        self.client = connect_to_database('zen4', 'idrac')
        
        if not self.client:
            print("❌ Failed to connect to ZEN4 database")
            return False
        
        print("✓ Connected to ZEN4 database")
        return True
    
    def disconnect(self):
        """Disconnect from database"""
        if self.client:
            disconnect_all()
    
    def get_power_metrics(self, limit: int = 1000):
        """Get ZEN4 power metrics with joins"""
        try:
            print("📊 Collecting ZEN4 power metrics...")
            
            results = {}
            
            # First, get the list of power metrics from the public schema
            from queries.compute.public import POWER_METRICS_QUERY_UNIT_IN_MW_W_KW
            
            # Connect to public schema to get power metrics list
            public_client = connect_to_database('zen4', 'public')
            if not public_client:
                print("❌ Failed to connect to zen4 public schema")
                return {}
            
            # Get power metrics from public schema
            df_power_metrics = pd.read_sql_query(POWER_METRICS_QUERY_UNIT_IN_MW_W_KW, public_client.db_connection)
            
            if df_power_metrics.empty:
                print("  - No power metrics found in public schema")
                return {}
            
            # Get metric IDs (convert to lowercase for idrac schema table names)
            power_metrics = df_power_metrics['metric_id'].str.lower().tolist()
            
            print(f"  - Found {len(power_metrics)} power metrics to process")
            
            # Process each power metric
            for metric in power_metrics:
                try:
                    # Create a modified query with intelligent power conversion
                    base_query = get_power_metrics_with_joins(metric, limit=limit)
                    modified_query = create_power_query_with_conversion(base_query, metric)
                    
                    df = pd.read_sql_query(modified_query, self.client.db_connection)
                    
                    if not df.empty:
                        # Convert timestamp to timezone-unaware for Excel compatibility
                        if 'timestamp' in df.columns:
                            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                        
                        # Use the original metric ID (with proper case) as sheet name
                        original_metric_id = df_power_metrics[df_power_metrics['metric_id'].str.lower() == metric]['metric_id'].iloc[0]
                        results[f'ZEN4_{original_metric_id}'] = df
                        print(f"  - {original_metric_id}: {len(df)} rows")
                    else:
                        print(f"  - {metric}: No data found")
                        
                except Exception as e:
                    print(f"  - Error processing {metric}: {e}")
                    continue
            
            return results
            
        except Exception as e:
            print(f"Error getting ZEN4 power metrics: {e}")
            return {}
    
    def get_power_analysis(self):
        """Get ZEN4 power analysis queries"""
        try:
            print("📊 Collecting ZEN4 power analysis...")
            
            results = {}
            
            # Node power comparison
            df_node_comparison = pd.read_sql_query(
                NODE_POWER_COMPARISON,
                self.client.db_connection
            )
            results['ZEN4_Node_Power_Comparison'] = df_node_comparison
            
            # Power efficiency analysis
            df_efficiency = pd.read_sql_query(
                POWER_EFFICIENCY_ANALYSIS,
                self.client.db_connection
            )
            results['ZEN4_Power_Efficiency_Analysis'] = df_efficiency
            
            # Power consumption trends
            df_consumption_trends = pd.read_sql_query(
                POWER_CONSUMPTION_TRENDS,
                self.client.db_connection
            )
            results['ZEN4_Power_Consumption_Trends'] = df_consumption_trends
            
            # High power usage alerts
            df_high_power = pd.read_sql_query(
                HIGH_POWER_USAGE_ALERTS,
                self.client.db_connection
            )
            results['ZEN4_High_Power_Usage_Alerts'] = df_high_power
            
            # Power efficiency by node
            df_efficiency_by_node = pd.read_sql_query(
                POWER_EFFICIENCY_BY_NODE,
                self.client.db_connection
            )
            results['ZEN4_Power_Efficiency_By_Node'] = df_efficiency_by_node
            
            return results
            
        except Exception as e:
            print(f"Error getting ZEN4 power analysis: {e}")
            return {}
    
    def get_time_range_analysis(self, hours: int = 24):
        """Get ZEN4 time range power analysis"""
        try:
            print(f"📊 Collecting ZEN4 {hours}-hour time range analysis...")
            
            results = {}
            
            # Get time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # Power summary for time range
            power_summary_query = f"""
            SELECT 
                nodeid,
                AVG(value) as avg_power,
                MIN(value) as min_power,
                MAX(value) as max_power,
                COUNT(*) as sample_count,
                STDDEV(value) as power_stddev
            FROM computepower 
            WHERE timestamp BETWEEN '{start_time.strftime("%Y-%m-%d %H:%M:%S")}' 
            AND '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
            GROUP BY nodeid
            ORDER BY avg_power DESC
            """
            
            df_power_summary = pd.read_sql_query(power_summary_query, self.client.db_connection)
            results[f'ZEN4_Power_Summary_{hours}h'] = df_power_summary
            
            # CPU power summary for time range
            cpu_summary_query = f"""
            SELECT 
                nodeid,
                AVG(value) as avg_cpu_power,
                MIN(value) as min_cpu_power,
                MAX(value) as max_cpu_power,
                COUNT(*) as sample_count
            FROM cpupower 
            WHERE timestamp BETWEEN '{start_time.strftime("%Y-%m-%d %H:%M:%S")}' 
            AND '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
            GROUP BY nodeid
            ORDER BY avg_cpu_power DESC
            """
            
            df_cpu_summary = pd.read_sql_query(cpu_summary_query, self.client.db_connection)
            results[f'ZEN4_CPU_Power_Summary_{hours}h'] = df_cpu_summary
            
            # GPU power summary for time range
            gpu_summary_query = f"""
            SELECT 
                nodeid,
                AVG(value) as avg_gpu_power,
                MIN(value) as min_gpu_power,
                MAX(value) as max_gpu_power,
                COUNT(*) as sample_count
            FROM gpu1power 
            WHERE timestamp BETWEEN '{start_time.strftime("%Y-%m-%d %H:%M:%S")}' 
            AND '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
            GROUP BY nodeid
            ORDER BY avg_gpu_power DESC
            """
            
            df_gpu_summary = pd.read_sql_query(gpu_summary_query, self.client.db_connection)
            results[f'ZEN4_GPU_Power_Summary_{hours}h'] = df_gpu_summary
            
            return results
            
        except Exception as e:
            print(f"Error getting ZEN4 time range analysis: {e}")
            return {}
    
    def get_recent_power_data(self, limit: int = 100):
        """Get recent ZEN4 power data"""
        try:
            print(f"📊 Collecting recent ZEN4 power data (last {limit} records)...")
            
            results = {}
            
            # Recent compute power data
            recent_compute_query = f"""
            SELECT timestamp, nodeid, value 
            FROM computepower 
            ORDER BY timestamp DESC 
            LIMIT {limit}
            """
            
            df_recent_compute = pd.read_sql_query(recent_compute_query, self.client.db_connection)
            if not df_recent_compute.empty:
                df_recent_compute['timestamp'] = df_recent_compute['timestamp'].dt.tz_localize(None)
            results['ZEN4_Recent_Compute_Power'] = df_recent_compute
            
            # Recent CPU power data
            recent_cpu_query = f"""
            SELECT timestamp, nodeid, value 
            FROM cpupower 
            ORDER BY timestamp DESC 
            LIMIT {limit}
            """
            
            df_recent_cpu = pd.read_sql_query(recent_cpu_query, self.client.db_connection)
            if not df_recent_cpu.empty:
                df_recent_cpu['timestamp'] = df_recent_cpu['timestamp'].dt.tz_localize(None)
            results['ZEN4_Recent_CPU_Power'] = df_recent_cpu
            
            # Recent GPU power data
            recent_gpu_query = f"""
            SELECT timestamp, nodeid, value 
            FROM gpu1power 
            ORDER BY timestamp DESC 
            LIMIT {limit}
            """
            
            df_recent_gpu = pd.read_sql_query(recent_gpu_query, self.client.db_connection)
            if not df_recent_gpu.empty:
                df_recent_gpu['timestamp'] = df_recent_gpu['timestamp'].dt.tz_localize(None)
            results['ZEN4_Recent_GPU_Power'] = df_recent_gpu
            
            return results
            
        except Exception as e:
            print(f"Error getting recent ZEN4 power data: {e}")
            return {}
    
    def create_excel_report(self, power_metrics, power_analysis, time_range_data, recent_data, output_filename=None):
        """Create Excel report with separate sheets for each dataset"""
        
        # Create output directory if it doesn't exist
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"zen4_power_queries_{timestamp}.xlsx"
        
        # Full path to output file
        output_path = os.path.join(output_dir, output_filename)
        
        # Combine all data
        all_data = {**power_metrics, **power_analysis, **time_range_data, **recent_data}
        
        # Filter out empty DataFrames
        all_data = {k: v for k, v in all_data.items() if not v.empty}
        
        # Check if we have any data to write
        if not all_data:
            print("❌ No data available to write to Excel report")
            return None
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                
                # Write power metrics data
                print("Writing ZEN4 power metrics...")
                for sheet_name, df in power_metrics.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write power analysis data
                print("Writing ZEN4 power analysis...")
                for sheet_name, df in power_analysis.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write time range data
                print("Writing ZEN4 time range analysis...")
                for sheet_name, df in time_range_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write recent data
                print("Writing ZEN4 recent power data...")
                for sheet_name, df in recent_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
            
            print(f"\nExcel report created successfully: {output_path}")
            return output_path
        
        except Exception as e:
            print(f"Error creating Excel report: {e}")
            return None


def main():
    """Main function to run all ZEN4 power queries"""
    print("🚀 ZEN4 Power Queries Runner")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print()
    
    runner = Zen4PowerQueryRunner()
    
    try:
        # Connect to ZEN4 database
        if not runner.connect_to_zen4():
            return
        
        # Get power metrics
        power_metrics = runner.get_power_metrics(limit=1000)
        
        # Get power analysis
        power_analysis = runner.get_power_analysis()
        
        # Get time range analysis
        time_range_data = runner.get_time_range_analysis(hours=24)
        
        # Get recent power data
        recent_data = runner.get_recent_power_data(limit=100)
        
        # Create Excel report
        print("\nCreating Excel report...")
        output_file = runner.create_excel_report(
            power_metrics, power_analysis, time_range_data, recent_data
        )
        
        if output_file:
            print(f"\nZEN4 power queries report summary:")
            print(f"  - Power metrics sheets: {len([k for k, v in power_metrics.items() if not v.empty])}")
            print(f"  - Power analysis sheets: {len([k for k, v in power_analysis.items() if not v.empty])}")
            print(f"  - Time range sheets: {len([k for k, v in time_range_data.items() if not v.empty])}")
            print(f"  - Recent data sheets: {len([k for k, v in recent_data.items() if not v.empty])}")
            print(f"  - Output file: {output_file}")
            
            # Print summary statistics
            print(f"\nData summary:")
            for sheet_name, df in power_metrics.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            
            for sheet_name, df in power_analysis.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            
            for sheet_name, df in time_range_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            
            for sheet_name, df in recent_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
        else:
            print("Failed to create Excel report")
        
        print("✅ All ZEN4 power queries completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        # Disconnect from database
        runner.disconnect()
        print(f"📅 Finished at: {datetime.now()}")


if __name__ == "__main__":
    main()
