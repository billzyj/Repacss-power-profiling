#!/usr/bin/env python3
"""
Run H100 Power Queries
Handles h100.idrac schema queries for H100 compute node power monitoring
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
import re
import matplotlib.pyplot as plt

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.compute.idrac import get_compute_metrics_with_joins
from core.database import (
    connect_to_database,
    disconnect_all
)
from core.power_utils import create_power_query_with_conversion, compute_energy_kwh_for_hostname

# Define metric constants directly to avoid import issues
CPU_METRICS = ['CPUPower', 'PkgPwr', 'TotalCPUPower']
MEMORY_METRICS = ['DRAMPwr', 'TotalMemoryPower'] 
GPU_METRICS = ['PowerConsumption']
FAN_METRICS = ['TotalFanPower']
STORAGE_METRICS = ['TotalStoragePower']
SYSTEM_METRICS = ['SystemInputPower', 'SystemOutputPower', 'SystemPowerConsumption', 'WattsReading']


class H100PowerQueryRunner:
    """Manages H100 power queries and analysis"""
    
    def __init__(self):
        self.client = None
        
    def connect_to_h100(self):
        """Connect to H100 database"""
        print("🔌 Connecting to H100 database...")
        self.client = connect_to_database('h100', 'idrac')
        
        if not self.client:
            print("❌ Failed to connect to H100 database")
            return False
        
        print("✓ Connected to H100 database")
        return True
    
    def disconnect(self):
        """Disconnect from database"""
        if self.client:
            disconnect_all()
    
    def get_power_metrics(self, limit: int = 1000):
        """Get H100 power metrics with joins"""
        try:
            print("📊 Collecting H100 power metrics...")
            
            results = {}
            
            # First, get the list of power metrics from the public schema
            from queries.compute.public import POWER_METRICS_QUERY_UNIT_IN_MW_W_KW
            
            # Connect to public schema to get power metrics list
            public_client = connect_to_database('h100', 'public')
            if not public_client:
                print("❌ Failed to connect to h100 public schema")
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
                    # Use the proper API for metric queries
                    query = get_compute_metrics_with_joins(
                        metric_id=metric,
                        hostname='rpg-93-1',  # H100 node
                        limit=limit
                    )
                    
                    df = pd.read_sql_query(query, self.client.db_connection)
                    
                    if not df.empty:
                        # Convert timestamp to timezone-unaware for Excel compatibility
                        if 'timestamp' in df.columns:
                            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                        
                        # Use the original metric ID (with proper case) as sheet name
                        original_metric_id = df_power_metrics[df_power_metrics['metric_id'].str.lower() == metric]['metric_id'].iloc[0]
                        results[f'H100_{original_metric_id}'] = df
                        print(f"  - {original_metric_id}: {len(df)} rows")
                    else:
                        print(f"  - {metric}: No data found")
                            
                except Exception as e:
                    print(f"  - Error processing {metric}: {e}")
                continue
                
            return results
            
        except Exception as e:
            print(f"Error getting H100 power metrics: {e}")
            return {}
    
    def get_power_analysis(self):
        """Get H100 power analysis queries"""
        try:
            print("📊 Collecting H100 power analysis...")
            
            results = {}
            
            # Use API for power analysis
            power_analysis_query = get_compute_metrics_with_joins(
                metric_id='computepower',
                hostname='rpg-93-1'  # H100 node
            )
            
            df_power_analysis = pd.read_sql_query(power_analysis_query, self.client.db_connection)
            
            # Calculate summary statistics
            if not df_power_analysis.empty:
                summary_stats = df_power_analysis.groupby('hostname').agg({
                    'value': ['mean', 'min', 'max', 'count']
                }).round(2)
                summary_stats.columns = ['avg_power', 'min_power', 'max_power', 'sample_count']
                summary_stats = summary_stats.reset_index()
                df_power_analysis = summary_stats
            results['H100_Power_Analysis'] = df_power_analysis
            
            return results
                        
        except Exception as e:
            print(f"Error getting H100 power analysis: {e}")
            return {}
    
    def get_time_range_analysis(self, start_time: datetime, end_time: datetime):
        """Get H100 time range power analysis for specific time period"""
        try:
            print(f"📊 Collecting H100 power analysis from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}...")
            
            results = {}
            
            # Use API for power summary with time range
            power_summary_query = get_compute_metrics_with_joins(
                metric_id='computepower',
                hostname='rpg-93-1',  # H100 node
                start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
            )
            
            df_power_summary = pd.read_sql_query(power_summary_query, self.client.db_connection)
            
            # Calculate summary statistics
            if not df_power_summary.empty:
                summary_stats = df_power_summary.groupby('hostname').agg({
                    'value': ['mean', 'min', 'max', 'count', 'std']
                }).round(2)
                summary_stats.columns = ['avg_power', 'min_power', 'max_power', 'sample_count', 'power_stddev']
                summary_stats = summary_stats.reset_index()
                df_power_summary = summary_stats
            results['H100_Power_Summary'] = df_power_summary
            
            # Use API for CPU power summary with time range
            cpu_summary_query = get_compute_metrics_with_joins(
                metric_id='cpupower',
                hostname='rpg-93-1',  # H100 node
                start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
            )
            
            df_cpu_summary = pd.read_sql_query(cpu_summary_query, self.client.db_connection)
            
            # Calculate summary statistics
            if not df_cpu_summary.empty:
                summary_stats = df_cpu_summary.groupby('hostname').agg({
                    'value': ['mean', 'min', 'max', 'count']
                }).round(2)
                summary_stats.columns = ['avg_cpu_power', 'min_cpu_power', 'max_cpu_power', 'sample_count']
                summary_stats = summary_stats.reset_index()
                df_cpu_summary = summary_stats
            results['H100_CPU_Power_Summary'] = df_cpu_summary
            
            # Use API for GPU power summary with time range
            gpu_summary_query = get_compute_metrics_with_joins(
                metric_id='gpu1power',
                hostname='rpg-93-1',  # H100 node
                start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
            )
            
            df_gpu_summary = pd.read_sql_query(gpu_summary_query, self.client.db_connection)
            
            # Calculate summary statistics
            if not df_gpu_summary.empty:
                summary_stats = df_gpu_summary.groupby('hostname').agg({
                    'value': ['mean', 'min', 'max', 'count']
                }).round(2)
                summary_stats.columns = ['avg_gpu_power', 'min_gpu_power', 'max_gpu_power', 'sample_count']
                summary_stats = summary_stats.reset_index()
                df_gpu_summary = summary_stats
            results['H100_GPU_Power_Summary'] = df_gpu_summary
            
            return results
            
        except Exception as e:
            print(f"Error getting H100 time range analysis: {e}")
            return {}
    
    def get_comprehensive_metric_analysis(self, start_time: datetime, end_time: datetime):
        """Get comprehensive metric analysis for H100 node 1"""
        try:
            print(f"📊 Collecting comprehensive H100 metric analysis...")
            
            results = {}
            node_id = 1
            
            # Get all metric data for the time period
            all_metrics = {
                'CPU': CPU_METRICS,
                'MEMORY': MEMORY_METRICS, 
                'GPU': GPU_METRICS,
                'FAN': FAN_METRICS,
                'STORAGE': STORAGE_METRICS,
                'SYSTEM': SYSTEM_METRICS
            }
            
            metric_data = {}
            energy_analysis = {}
            
            # Collect data for each metric category
            for category, metrics in all_metrics.items():
                print(f"  📈 Processing {category} metrics...")
                category_data = {}
                category_energy = {}
                
                for metric in metrics:
                    table_name = metric.lower()
                    print(f"    - {metric} (table: {table_name})")
                    
                    # Use the proper API for metric queries
                    query = get_compute_metrics_with_joins(
                        metric_id=metric,
                        hostname='rpg-93-1',  # H100 node
                        start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
                    )
                    
                    try:
                        df = pd.read_sql_query(query, self.client.db_connection)
                        if not df.empty and 'hostname' in df.columns:
                            # Convert timestamp to timezone-unaware for Excel compatibility
                            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                            # Ensure expected columns exist for downstream checks/output
                            expected_cols = ['timestamp','hostname','source','fqdd','value','units']
                            for col in expected_cols:
                                if col not in df.columns:
                                    # Create missing optional cols to avoid downstream errors
                                    if col in ['source','fqdd','units']:
                                        df[col] = None
                                    elif col == 'hostname':
                                        df[col] = 'rpg-93-1'
                            
                            # Group by FQDD and calculate totals
                            if category in ['CPU', 'GPU']:
                                # Keep individual FQDD data
                                df['metric'] = metric
                                category_data[f'{metric}_BY_FQDD'] = df

                                if category == 'GPU':
                                    # H100 GPUs: 4 FQDDs; compute energy per FQDD first, then sum energies
                                    gpu_total_energy = 0.0
                                    fqdd_energies = []
                                    # Use FQDD values exactly as reported; do not remap or normalize
                                    for fq in df['fqdd'].dropna().unique():
                                        fq_df = df[df['fqdd'] == fq].copy()
                                        # Collapse duplicate timestamps per FQDD if any
                                        fq_df = fq_df.groupby(['timestamp','hostname','fqdd'], as_index=False)['value'].sum()
                                        # PowerConsumption in idrac is often reported in mW; if missing, default to mW
                                        unit_str = (fq_df['units'].iloc[0] if 'units' in fq_df.columns and fq_df['units'].notna().any() else 'mW')
                                        fq_energy = compute_energy_kwh_for_hostname(
                                            fq_df, unit_str, 'rpg-93-1',
                                            start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                            end_time.strftime('%Y-%m-%d %H:%M:%S')
                                        )
                                        gpu_total_energy += fq_energy
                                        fqdd_energies.append({'fqdd': fq, 'energy_kwh': fq_energy})

                                    # Build a TOTAL time series by summing across FQDD at each timestamp for reference/output
                                    df_grouped = df.groupby('timestamp', as_index=False)['value'].sum()
                                    df_grouped['hostname'] = df['hostname'].iloc[0]
                                    df_grouped['fqdd'] = 'TOTAL'
                                    df_grouped['units'] = df['units'].iloc[0] if 'units' in df.columns else None
                                    df_grouped['metric'] = metric
                                    category_data[f'{metric}_TOTAL'] = df_grouped
                                    # Store energies: per-FQDD and TOTAL
                                    for item in fqdd_energies:
                                        category_energy[f'{metric}_FQDD_{item["fqdd"]}'] = item['energy_kwh']
                                    category_energy[f'{metric}_TOTAL'] = gpu_total_energy
                                else:
                                    # CPU: compute per-FQDD energies first (default unit W), then build TOTAL time series
                                    cpu_total_energy = 0.0
                                    for fq in df['fqdd'].dropna().unique():
                                        fq_df = df[df['fqdd'] == fq].copy()
                                        fq_df = fq_df.groupby(['timestamp','hostname','fqdd'], as_index=False)['value'].sum()
                                        unit_str_fq = (fq_df['units'].iloc[0] if 'units' in fq_df.columns and fq_df['units'].notna().any() else 'W')
                                        fq_energy = compute_energy_kwh_for_hostname(
                                            fq_df, unit_str_fq, 'rpg-93-1',
                                            start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                            end_time.strftime('%Y-%m-%d %H:%M:%S')
                                        )
                                        category_energy[f'CPUPower_FQDD_{fq}'] = fq_energy
                                        cpu_total_energy += fq_energy

                                    # TOTAL series (sum across FQDDs at each timestamp)
                                    df_grouped = df.groupby('timestamp', as_index=False)['value'].sum()
                                    df_grouped['hostname'] = df['hostname'].iloc[0]
                                    df_grouped['fqdd'] = 'TOTAL'
                                    df_grouped['units'] = df['units'].iloc[0] if 'units' in df.columns else None
                                    df_grouped['metric'] = metric
                                    category_data[f'{metric}_TOTAL'] = df_grouped
                                    unit_str = (df_grouped['units'].iloc[0] if 'units' in df_grouped.columns and df_grouped['units'].notna().any()
                                                else (df['units'].iloc[0] if 'units' in df.columns and df['units'].notna().any() else 'W'))
                                    total_energy = compute_energy_kwh_for_hostname(
                                        df_grouped, unit_str, 'rpg-93-1',
                                        start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                        end_time.strftime('%Y-%m-%d %H:%M:%S')
                                    )
                                    # Prefer per-FQDD sum if available
                                    category_energy[f'{metric}_TOTAL'] = cpu_total_energy if cpu_total_energy > 0 else total_energy
                                
                            else:
                                # For other metrics, keep as is
                                df['metric'] = metric
                                category_data[metric] = df
                                
                                # Calculate energy
                                unit_str = (df['units'].iloc[0] if 'units' in df.columns and df['units'].notna().any() else 'W')
                                total_energy = compute_energy_kwh_for_hostname(
                                    df, unit_str, 'rpg-93-1',
                                    start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                    end_time.strftime('%Y-%m-%d %H:%M:%S')
                                )
                                category_energy[metric] = total_energy
                                
                            print(f"      ✓ {metric}: {len(df)} rows, Energy: {category_energy.get(f'{metric}_TOTAL', category_energy.get(metric, 0)):.3f} kWh")
                        else:
                            print(f"      ⚠️ {metric}: No data found or missing hostname column")
                            
                    except Exception as e:
                        print(f"      ❌ Error processing {metric}: {e}")
                        continue
                
                metric_data[category] = category_data
                energy_analysis[category] = category_energy
            
            # Create metric relationship analysis
            relationship_analysis = []
            
            # CPU metrics comparison
            cpu_metrics = list(energy_analysis['CPU'].keys())
            if len(cpu_metrics) > 1:
                for i, metric1 in enumerate(cpu_metrics):
                    for metric2 in cpu_metrics[i+1:]:
                        energy1 = energy_analysis['CPU'][metric1]
                        energy2 = energy_analysis['CPU'][metric2]
                        if energy1 > 0 and energy2 > 0:
                            diff_pct = abs(energy1 - energy2) / max(energy1, energy2) * 100
                            relationship_analysis.append({
                                'category': 'CPU',
                                'metric1': metric1,
                                'metric2': metric2,
                                'energy1_kwh': energy1,
                                'energy2_kwh': energy2,
                                'difference_kwh': abs(energy1 - energy2),
                                'difference_percent': diff_pct,
                                'relationship': 'IDENTICAL' if diff_pct < 5 else 'DIFFERENT'
                            })
            
            # Memory metrics comparison
            mem_metrics = list(energy_analysis['MEMORY'].keys())
            if len(mem_metrics) > 1:
                for i, metric1 in enumerate(mem_metrics):
                    for metric2 in mem_metrics[i+1:]:
                        energy1 = energy_analysis['MEMORY'][metric1]
                        energy2 = energy_analysis['MEMORY'][metric2]
                        if energy1 > 0 and energy2 > 0:
                            diff_pct = abs(energy1 - energy2) / max(energy1, energy2) * 100
                            relationship_analysis.append({
                                'category': 'MEMORY',
                                'metric1': metric1,
                                'metric2': metric2,
                                'energy1_kwh': energy1,
                                'energy2_kwh': energy2,
                                'difference_kwh': abs(energy1 - energy2),
                                'difference_percent': diff_pct,
                                'relationship': 'IDENTICAL' if diff_pct < 5 else 'DIFFERENT'
                            })
            
            # System metrics comparison
            sys_metrics = list(energy_analysis['SYSTEM'].keys())
            if len(sys_metrics) > 1:
                for i, metric1 in enumerate(sys_metrics):
                    for metric2 in sys_metrics[i+1:]:
                        energy1 = energy_analysis['SYSTEM'][metric1]
                        energy2 = energy_analysis['SYSTEM'][metric2]
                        if energy1 > 0 and energy2 > 0:
                            diff_pct = abs(energy1 - energy2) / max(energy1, energy2) * 100
                            relationship_analysis.append({
                                'category': 'SYSTEM',
                                'metric1': metric1,
                                'metric2': metric2,
                                'energy1_kwh': energy1,
                                'energy2_kwh': energy2,
                                'difference_kwh': abs(energy1 - energy2),
                                'difference_percent': diff_pct,
                                'relationship': 'IDENTICAL' if diff_pct < 5 else 'DIFFERENT'
                            })
            
            # System output vs components validation
            system_output_energy = energy_analysis['SYSTEM'].get('SystemOutputPower', 0)
            component_total = 0
            component_breakdown = {}
            component_breakdown_rows = []
            
            # Prefer TOTAL metrics for each component to avoid double counting
            # CPU: prefer TotalCPUPower if present
            cpu_energy = None
            for key, val in energy_analysis['CPU'].items():
                if key.lower().startswith('totalcpupower'):
                    cpu_energy = val
                    break
            if cpu_energy is None:
                cpu_energy = sum(energy_analysis['CPU'].values())
            component_breakdown['CPU'] = cpu_energy
            component_breakdown_rows.append({'component': 'CPU', 'energy_kwh': cpu_energy})
            component_total += cpu_energy

            # MEMORY: prefer TotalMemoryPower
            mem_energy = None
            for key, val in energy_analysis['MEMORY'].items():
                if key.lower().startswith('totalmemorypower'):
                    mem_energy = val
                    break
            if mem_energy is None:
                mem_energy = sum(energy_analysis['MEMORY'].values())
            component_breakdown['MEMORY'] = mem_energy
            component_breakdown_rows.append({'component': 'MEMORY', 'energy_kwh': mem_energy})
            component_total += mem_energy

            # GPU: prefer sum of per-FQDD energies; keep original FQDD labels
            # GPU per-FQDD energies and total
            gpu_total_energy = 0.0
            for key, val in energy_analysis['GPU'].items():
                if key.startswith('PowerConsumption_FQDD_'):
                    suffix = str(key.split('_')[-1]).strip()
                    label = f"GPU (fqdd={suffix})"
                    component_breakdown_rows.append({'component': label, 'energy_kwh': val})
                    gpu_total_energy += val
            # Fallback: if no per-FQDD entries were recorded, use TOTAL or sum of all GPU energies
            if gpu_total_energy == 0.0:
                total_from_key = None
                for key, val in energy_analysis['GPU'].items():
                    if key.endswith('_TOTAL'):
                        total_from_key = val
                        break
                gpu_total_energy = total_from_key if total_from_key is not None else sum(energy_analysis['GPU'].values())
            component_breakdown['GPU Total'] = gpu_total_energy
            component_total += gpu_total_energy
            component_breakdown_rows.append({'component': 'GPU Total', 'energy_kwh': gpu_total_energy})

            # FAN: prefer TotalFanPower
            fan_energy = None
            for key, val in energy_analysis['FAN'].items():
                if key.lower().startswith('totalfanpower'):
                    fan_energy = val
                    break
            if fan_energy is None:
                fan_energy = sum(energy_analysis['FAN'].values())
            component_breakdown['FAN'] = fan_energy
            component_breakdown_rows.append({'component': 'FAN', 'energy_kwh': fan_energy})
            component_total += fan_energy

            # STORAGE: prefer TotalStoragePower
            storage_energy = None
            for key, val in energy_analysis['STORAGE'].items():
                if key.lower().startswith('totalstoragepower'):
                    storage_energy = val
                    break
            if storage_energy is None:
                storage_energy = sum(energy_analysis['STORAGE'].values())
            component_breakdown['STORAGE'] = storage_energy
            component_breakdown_rows.append({'component': 'STORAGE', 'energy_kwh': storage_energy})
            component_total += storage_energy
            
            if system_output_energy > 0 and component_total > 0:
                validation_diff = abs(system_output_energy - component_total)
                validation_pct = validation_diff / system_output_energy * 100
                
                relationship_analysis.append({
                    'category': 'VALIDATION',
                    'metric1': 'SystemOutputPower',
                    'metric2': 'Components_Total',
                    'energy1_kwh': system_output_energy,
                    'energy2_kwh': component_total,
                    'difference_kwh': validation_diff,
                    'difference_percent': validation_pct,
                    'relationship': 'MATCHES' if validation_pct < 10 else 'MISMATCH'
                })
            
            # Store results
            results['Metric_Data'] = metric_data
            results['Energy_Analysis'] = energy_analysis
            results['Relationship_Analysis'] = pd.DataFrame(relationship_analysis)
            # Emit detailed component breakdown including GPU per-FQDD and totals
            component_df = pd.DataFrame(component_breakdown_rows)
            
            # Add SystemOutputPower as reference row
            component_df = pd.concat([
                component_df,
                pd.DataFrame([{'component': 'SystemOutputPower', 'energy_kwh': system_output_energy}])
            ], ignore_index=True)
            
            # Reorder rows: CPU, MEMORY, all GPU entries as reported, FAN, STORAGE, SystemOutputPower
            gpu_labels_order = [lbl for lbl in component_df['component'] if lbl.startswith('GPU')]
            desired_order = ['CPU', 'MEMORY'] + gpu_labels_order + ['FAN', 'STORAGE', 'SystemOutputPower']
            # Ensure all desired rows exist
            for comp in desired_order:
                if comp not in set(component_df['component']):
                    component_df = pd.concat([
                        component_df,
                        pd.DataFrame([{'component': comp, 'energy_kwh': 0.0}])
                    ], ignore_index=True)
            component_df['order_idx'] = component_df['component'].apply(
                lambda x: desired_order.index(x) if x in desired_order else len(desired_order)+1
            )
            component_df = component_df.sort_values(['order_idx', 'component']).drop(columns=['order_idx'])
            
            # Build top-level pie: GPU Total, CPU Total, MEMORY, FAN, STORAGE, Other (GPU per-FQDD NOT included here)
            top_rows = []
            # CPU total from breakdown
            cpu_total_val = next((v for k, v in component_breakdown.items() if k == 'CPU'), 0.0)
            # GPU total from breakdown
            gpu_total_val = next((v for k, v in component_breakdown.items() if k == 'GPU Total'), 0.0)
            mem_val = component_breakdown.get('MEMORY', 0.0)
            fan_val = component_breakdown.get('FAN', 0.0)
            storage_val = component_breakdown.get('STORAGE', 0.0)
            comp_sum_no_other = cpu_total_val + gpu_total_val + mem_val + fan_val + storage_val
            other_val = max(system_output_energy - comp_sum_no_other, 0.0)
            top_rows.extend([
                {'component':'GPU Total','energy_kwh':gpu_total_val},
                {'component':'CPU','energy_kwh':cpu_total_val},
                {'component':'MEMORY','energy_kwh':mem_val},
                {'component':'FAN','energy_kwh':fan_val},
                {'component':'STORAGE','energy_kwh':storage_val},
                {'component':'Other','energy_kwh':other_val},
            ])
            top_pie_df = pd.DataFrame(top_rows)
            
            # Build CPU breakdown pie (per FQDD)
            cpu_fqdd_rows = []
            for key, val in energy_analysis['CPU'].items():
                if key.startswith('CPUPower_FQDD_'):
                    fq = str(key.split('_')[-1])
                    cpu_fqdd_rows.append({'component': f'CPU (fqdd={fq})', 'energy_kwh': val})
            cpu_pie_df = pd.DataFrame(cpu_fqdd_rows)
            
            # Generate and save pie chart
            try:
                fig, axes = plt.subplots(1, 2, figsize=(14, 7))
                # Left: Top-level
                labels_top = top_pie_df['component'].tolist()
                sizes_top = top_pie_df['energy_kwh'].tolist()
                if sum(sizes_top) == 0:
                    sizes_top = [1 for _ in sizes_top]
                explode_top = [0.06 if c == 'GPU Total' else 0 for c in labels_top]
                axes[0].pie(sizes_top, labels=labels_top, autopct='%1.1f%%', startangle=140, explode=explode_top)
                axes[0].set_title('Top-level Energy Share')
                axes[0].axis('equal')
                # Right: CPU per-FQDD
                if not cpu_pie_df.empty:
                    labels_cpu = cpu_pie_df['component'].tolist()
                    sizes_cpu = cpu_pie_df['energy_kwh'].tolist()
                    if sum(sizes_cpu) == 0:
                        sizes_cpu = [1 for _ in sizes_cpu]
                    axes[1].pie(sizes_cpu, labels=labels_cpu, autopct='%1.1f%%', startangle=140)
                    axes[1].set_title('CPU Energy by FQDD')
                    axes[1].axis('equal')
                else:
                    axes[1].text(0.5, 0.5, 'No CPU FQDD data', ha='center', va='center')
                    axes[1].axis('off')
                
                output_dir = "output/h100"
                os.makedirs(output_dir, exist_ok=True)
                pie_filename = f"h100_component_breakdown_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                pie_path = os.path.join(output_dir, pie_filename)
                fig.savefig(pie_path, bbox_inches='tight')
                plt.close(fig)
            except Exception as e:
                pie_path = None
                print(f"⚠️ Failed to create/save component pie chart: {e}")

            results['Component_Breakdown'] = component_df
            results['Component_Pie_Path'] = pie_path
            
            return results
            
        except Exception as e:
            print(f"Error getting comprehensive metric analysis: {e}")
            return {}
    
    def get_recent_power_data(self, limit: int = 100):
        """Get recent H100 power data"""
        try:
            print(f"📊 Collecting recent H100 power data (last {limit} records)...")
            
            results = {}
            
            # Use API for recent compute power data
            recent_compute_query = get_compute_metrics_with_joins(
                metric_id='computepower',
                hostname='rpg-93-1',  # H100 node
                limit=limit
            )
            
            df_recent_compute = pd.read_sql_query(recent_compute_query, self.client.db_connection)
            if not df_recent_compute.empty:
                df_recent_compute['timestamp'] = df_recent_compute['timestamp'].dt.tz_localize(None)
            results['H100_Recent_Compute_Power'] = df_recent_compute
            
            # Use API for recent CPU power data
            recent_cpu_query = get_compute_metrics_with_joins(
                metric_id='cpupower',
                hostname='rpg-93-1',  # H100 node
                limit=limit
            )
            
            df_recent_cpu = pd.read_sql_query(recent_cpu_query, self.client.db_connection)
            if not df_recent_cpu.empty:
                df_recent_cpu['timestamp'] = df_recent_cpu['timestamp'].dt.tz_localize(None)
            results['H100_Recent_CPU_Power'] = df_recent_cpu
            
            # Use API for recent GPU power data
            recent_gpu_query = get_compute_metrics_with_joins(
                metric_id='gpu1power',
                hostname='rpg-93-1',  # H100 node
                limit=limit
            )
            
            df_recent_gpu = pd.read_sql_query(recent_gpu_query, self.client.db_connection)
            if not df_recent_gpu.empty:
                df_recent_gpu['timestamp'] = df_recent_gpu['timestamp'].dt.tz_localize(None)
            results['H100_Recent_GPU_Power'] = df_recent_gpu
            
            return results
            
        except Exception as e:
            print(f"Error getting recent H100 power data: {e}")
            return {}
    
    def create_excel_report(self, power_metrics, power_analysis, time_range_data, recent_data, output_filename=None):
        """Create Excel report with separate sheets for each dataset"""
        
        # Create output directory if it doesn't exist
        output_dir = "output/h100"
        os.makedirs(output_dir, exist_ok=True)
        
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"h100_power_queries_{timestamp}.xlsx"
        
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
                print("Writing H100 power metrics...")
                for sheet_name, df in power_metrics.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write power analysis data
                print("Writing H100 power analysis...")
                for sheet_name, df in power_analysis.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write time range data
                print("Writing H100 time range analysis...")
                for sheet_name, df in time_range_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write recent data
                print("Writing H100 recent power data...")
                for sheet_name, df in recent_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
            
            print(f"\nExcel report created successfully: {output_path}")
            return output_path
                        
        except Exception as e:
            print(f"Error creating Excel report: {e}")
            return None
    
    def create_comprehensive_excel_report(self, comprehensive_analysis, time_range_data, recent_data, output_filename=None):
        """Create comprehensive Excel report with metric analysis"""
        
        # Create output directory if it doesn't exist
        output_dir = "output/h100"
        os.makedirs(output_dir, exist_ok=True)
        
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"h100_comprehensive_analysis_{timestamp}.xlsx"
        
        # Full path to output file
        output_path = os.path.join(output_dir, output_filename)
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                
                # Write relationship analysis
                if 'Relationship_Analysis' in comprehensive_analysis and not comprehensive_analysis['Relationship_Analysis'].empty:
                    comprehensive_analysis['Relationship_Analysis'].to_excel(
                        writer, sheet_name='Metric_Relationships', index=False
                    )
                    print(f"  - Metric_Relationships: {len(comprehensive_analysis['Relationship_Analysis'])} rows")
                
                # Write component breakdown
                if 'Component_Breakdown' in comprehensive_analysis and not comprehensive_analysis['Component_Breakdown'].empty:
                    comprehensive_analysis['Component_Breakdown'].to_excel(
                        writer, sheet_name='Component_Breakdown', index=False
                    )
                    print(f"  - Component_Breakdown: {len(comprehensive_analysis['Component_Breakdown'])} rows")
                
                # Write metric data for each category
                if 'Metric_Data' in comprehensive_analysis:
                    for category, category_data in comprehensive_analysis['Metric_Data'].items():
                        for metric_name, df in category_data.items():
                            if not df.empty:
                                sheet_name = f"{category}_{metric_name}"[:31]  # Excel sheet name limit
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                                print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write energy analysis summary
                if 'Energy_Analysis' in comprehensive_analysis:
                    energy_summary = []
                    for category, metrics in comprehensive_analysis['Energy_Analysis'].items():
                        for metric, energy in metrics.items():
                            energy_summary.append({
                                'category': category,
                                'metric': metric,
                                'energy_kwh': energy
                            })
                    
                    if energy_summary:
                        energy_df = pd.DataFrame(energy_summary)
                        energy_df.to_excel(writer, sheet_name='Energy_Summary', index=False)
                        print(f"  - Energy_Summary: {len(energy_df)} rows")
                
                # Write time range data
                print("Writing H100 time range analysis...")
                for sheet_name, df in time_range_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write recent data
                print("Writing H100 recent power data...")
                for sheet_name, df in recent_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
            
            print(f"\nComprehensive Excel report created successfully: {output_path}")
            return output_path
                        
        except Exception as e:
            print(f"Error creating comprehensive Excel report: {e}")
            return None


def main():
    """Main function to run all H100 power queries"""
    print("🚀 H100 Power Queries Runner")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print()
    
    # Set specific time period
    start_time = datetime(2025, 9, 1, 0, 0, 0)
    end_time = datetime(2025, 9, 2, 0, 0, 0)
    
    print(f"📊 Analyzing H100 power data from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Focus: rpg-93-1 (H100 node) with comprehensive metric analysis")
    print()
    
    runner = H100PowerQueryRunner()
    
    try:
        # Connect to H100 database
        if not runner.connect_to_h100():
            return
        
        # Get comprehensive metric analysis
        comprehensive_analysis = runner.get_comprehensive_metric_analysis(start_time, end_time)
        
        # Get time range analysis
        time_range_data = runner.get_time_range_analysis(start_time, end_time)
        
        # Get recent power data
        recent_data = runner.get_recent_power_data(limit=100)
        
        # Create Excel report
        print("\nCreating Excel report...")
        output_file = runner.create_comprehensive_excel_report(
            comprehensive_analysis, time_range_data, recent_data
        )
        
        if output_file:
            print(f"\nH100 comprehensive analysis report summary:")
            print(f"  - Comprehensive analysis sheets: {len(comprehensive_analysis.get('Metric_Data', {}))}")
            print(f"  - Time range sheets: {len([k for k, v in time_range_data.items() if not v.empty])}")
            print(f"  - Recent data sheets: {len([k for k, v in recent_data.items() if not v.empty])}")
            print(f"  - Output file: {output_file}")
            
            # Print energy analysis summary
            if 'Energy_Analysis' in comprehensive_analysis:
                print(f"\nEnergy Analysis Summary:")
                for category, metrics in comprehensive_analysis['Energy_Analysis'].items():
                    print(f"  {category}:")
                    for metric, energy in metrics.items():
                        print(f"    - {metric}: {energy:.3f} kWh")
            
            # Print relationship analysis summary
            if 'Relationship_Analysis' in comprehensive_analysis and not comprehensive_analysis['Relationship_Analysis'].empty:
                print(f"\nMetric Relationship Analysis:")
                for _, row in comprehensive_analysis['Relationship_Analysis'].iterrows():
                    print(f"  - {row['category']}: {row['metric1']} vs {row['metric2']} - {row['relationship']} ({row['difference_percent']:.1f}% diff)")
            
            # Print component breakdown
            if 'Component_Breakdown' in comprehensive_analysis and not comprehensive_analysis['Component_Breakdown'].empty:
                print(f"\nComponent Energy Breakdown:")
                for _, row in comprehensive_analysis['Component_Breakdown'].iterrows():
                    print(f"  - {row['component']}: {row['energy_kwh']:.3f} kWh")
            if comprehensive_analysis.get('Component_Pie_Path'):
                print(f"  - Pie chart saved to: {comprehensive_analysis['Component_Pie_Path']}")
        else:
            print("Failed to create Excel report")
        
        print("✅ All H100 power queries completed successfully!")
        
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
