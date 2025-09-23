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
import matplotlib.pyplot as plt

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.compute.idrac import get_compute_metrics_with_joins
from core.database import (
    connect_to_database,
    disconnect_all
)
from core.power_utils import create_power_query_with_conversion, compute_energy_kwh_for_hostname

# Define metric constants (GPU omitted for Zen4)
CPU_METRICS = ['CPUPower', 'PkgPwr', 'TotalCPUPower']
MEMORY_METRICS = ['DRAMPwr', 'TotalMemoryPower']
FAN_METRICS = ['TotalFanPower']
STORAGE_METRICS = ['TotalStoragePower']
SYSTEM_METRICS = ['SystemInputPower','SystemOutputPower', 'SystemPowerConsumption', 'WattsReading']


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
            
            # Process each power metric (using idrac API)
            for metric in power_metrics:
                try:
                    query = get_compute_metrics_with_joins(metric_id=metric, limit=limit)
                    df = pd.read_sql_query(query, self.client.db_connection)
                    
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
    
    def get_comprehensive_metric_analysis(self, start_time: datetime, end_time: datetime):
        """Get comprehensive metric analysis for ZEN4 node rpc-91-1 (GPU omitted)"""
        try:
            print(f"📊 Collecting ZEN4 comprehensive metric analysis...")
            results = {}
            hostname = 'rpc-91-1'

            all_metrics = {
                'CPU': CPU_METRICS,
                'MEMORY': MEMORY_METRICS,
                'FAN': FAN_METRICS,
                'STORAGE': STORAGE_METRICS,
                'SYSTEM': SYSTEM_METRICS
            }

            metric_data: Dict[str, Dict[str, pd.DataFrame]] = {}
            energy_analysis: Dict[str, Dict[str, float]] = {}

            for category, metrics in all_metrics.items():
                print(f"  📈 Processing {category} metrics...")
                category_data: Dict[str, pd.DataFrame] = {}
                category_energy: Dict[str, float] = {}

                for metric in metrics:
                    query = get_compute_metrics_with_joins(
                        metric_id=metric,
                        hostname=hostname,
                        start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
                    )
                    try:
                        df = pd.read_sql_query(query, self.client.db_connection)
                        if not df.empty:
                            # Normalize timestamps for energy integration
                            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
                            df = df.dropna(subset=['timestamp']).sort_values('timestamp')
                            # Keep a DatetimeIndex to satisfy any resample operations
                            df = df.set_index('timestamp').reset_index()
                            # Excel compatibility: make timestamps timezone-naive
                            if 'timestamp' in df.columns:
                                df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                            expected_cols = ['timestamp','hostname','source','fqdd','value','units']
                            for col in expected_cols:
                                if col not in df.columns:
                                    df[col] = None if col in ['source','fqdd','units'] else hostname

                            df['metric'] = metric
                            category_data[metric] = df

                            unit_str = (df['units'].iloc[0] if 'units' in df.columns and df['units'].notna().any() else 'W')
                            energy = compute_energy_kwh_for_hostname(
                                df, unit_str, hostname,
                                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                end_time.strftime('%Y-%m-%d %H:%M:%S')
                            )
                            category_energy[metric] = energy
                            print(f"    ✓ {metric}: {len(df)} rows, Energy: {energy:.3f} kWh")
                        else:
                            print(f"    ⚠️ {metric}: No data found")
                    except Exception as e:
                        print(f"    ❌ Error processing {metric}: {e}")
                        continue

                metric_data[category] = category_data
                energy_analysis[category] = category_energy

            # Build component totals (prefer TOTAL metrics where available)
            def pick_total_or_sum(cat: str, total_prefix: str) -> float:
                for key, val in energy_analysis.get(cat, {}).items():
                    if key.lower().startswith(total_prefix.lower()):
                        return val
                return sum(energy_analysis.get(cat, {}).values())

            cpu_total = pick_total_or_sum('CPU', 'TotalCPUPower')
            mem_total = pick_total_or_sum('MEMORY', 'TotalMemoryPower')
            fan_total = pick_total_or_sum('FAN', 'TotalFanPower')
            storage_total = pick_total_or_sum('STORAGE', 'TotalStoragePower')
            system_output = energy_analysis.get('SYSTEM', {}).get('SystemOutputPower', 0.0)

            component_rows = [
                {'component':'CPU','energy_kwh':cpu_total},
                {'component':'MEMORY','energy_kwh':mem_total},
                {'component':'FAN','energy_kwh':fan_total},
                {'component':'STORAGE','energy_kwh':storage_total},
                {'component':'SystemOutputPower','energy_kwh':system_output},
            ]
            comp_df = pd.DataFrame(component_rows)

            # Top-level pie (no GPU): CPU, MEM, FAN, STORAGE, Other
            comp_sum = cpu_total + mem_total + fan_total + storage_total
            other_val = max(system_output - comp_sum, 0.0)
            top_pie_df = pd.DataFrame([
                {'component':'CPU','energy_kwh':cpu_total},
                {'component':'MEMORY','energy_kwh':mem_total},
                {'component':'FAN','energy_kwh':fan_total},
                {'component':'STORAGE','energy_kwh':storage_total},
                {'component':'Other','energy_kwh':other_val},
            ])

            # CPU FQDD pie
            cpu_fqdd_rows = []
            for metric, energy in energy_analysis.get('CPU', {}).items():
                if metric.startswith('CPUPower_FQDD_'):
                    fq = metric.split('_')[-1]
                    cpu_fqdd_rows.append({'component': f'CPU (fqdd={fq})', 'energy_kwh': energy})
            cpu_pie_df = pd.DataFrame(cpu_fqdd_rows)

            # Save pie (top-level only)
            pie_path = None
            try:
                plt.figure(figsize=(10, 7))
                labels_top = top_pie_df['component'].tolist()
                sizes_top = top_pie_df['energy_kwh'].tolist()
                if sum(sizes_top) == 0:
                    sizes_top = [1 for _ in sizes_top]
                plt.pie(sizes_top, labels=labels_top, autopct='%1.1f%%', startangle=140)
                plt.title('ZEN4 Top-level Energy Share (rpc-91-1)')
                plt.axis('equal')

                output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'output', 'zen4')
                os.makedirs(output_dir, exist_ok=True)
                pie_filename = f"zen4_component_breakdown_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                pie_path = os.path.join(output_dir, pie_filename)
                plt.savefig(pie_path, bbox_inches='tight')
                plt.close()
            except Exception as e:
                print(f"⚠️ Failed to create/save ZEN4 component pie chart: {e}")

            results['Metric_Data'] = metric_data
            results['Energy_Analysis'] = energy_analysis
            results['Component_Breakdown'] = comp_df
            results['Component_Pie_Path'] = pie_path
            
            return results
        except Exception as e:
            print(f"Error getting ZEN4 comprehensive metric analysis: {e}")
            return {}
    
    def get_power_analysis(self):
        """Kept for compatibility; returns empty for simplified flow"""
        print("📊 Skipping legacy ZEN4 power analysis (using comprehensive analysis instead)...")
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
    
    # Time window to match H100 analysis
    start_time = datetime(2025, 9, 1, 0, 0, 0)
    end_time = datetime(2025, 9, 2, 0, 0, 0)
    print(f"📊 Analyzing ZEN4 power data from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')} (rpc-91-1)")
    
    runner = Zen4PowerQueryRunner()
    
    try:
        # Connect to ZEN4 database
        if not runner.connect_to_zen4():
            return
        
        # Comprehensive metric analysis (GPU omitted)
        comp = runner.get_comprehensive_metric_analysis(start_time, end_time)
        
        # Minimal time-range and recent sections can be skipped; focus on comprehensive output
        print("\nCreating ZEN4 comprehensive report...")
        # Reuse H100-style exporter pattern: write a small Excel with breakdown
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'output', 'zen4')
        os.makedirs(output_dir, exist_ok=True)
        excel_path = os.path.join(output_dir, f"zen4_comprehensive_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Component breakdown
            if comp.get('Component_Breakdown') is not None and not comp['Component_Breakdown'].empty:
                comp['Component_Breakdown'].to_excel(writer, sheet_name='Component_Breakdown', index=False)
            # Raw metric data
            for cat, items in comp.get('Metric_Data', {}).items():
                for name, df in items.items():
                    if not df.empty:
                        # Ensure timezone-naive timestamps for Excel
                        if 'timestamp' in df.columns:
                            try:
                                df = df.copy()
                                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                                df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                            except Exception:
                                pass
                        df.to_excel(writer, sheet_name=(f"{cat}_{name}")[:31], index=False)
            # Energy analysis
            if comp.get('Energy_Analysis'):
                rows = []
                for cat, items in comp['Energy_Analysis'].items():
                    for name, val in items.items():
                        rows.append({'category':cat, 'metric':name, 'energy_kwh':val})
                pd.DataFrame(rows).to_excel(writer, sheet_name='Energy_Summary', index=False)

        print(f"  - Excel saved: {excel_path}")
        if comp.get('Component_Pie_Path'):
            print(f"  - Pie chart saved: {comp['Component_Pie_Path']}")
        
        print("✅ ZEN4 comprehensive analysis completed successfully!")
        
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
