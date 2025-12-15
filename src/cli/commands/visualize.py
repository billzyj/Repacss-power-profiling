#!/usr/bin/env python3
"""
Power visualization CLI command - query by nodeid and generate plots
"""

import click
import sys
import os
from datetime import datetime
from typing import Dict, List
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from queries.compute.idrac import get_compute_metrics_with_joins
from queries.compute.public import POWER_METRICS_QUERY_UNIT_IN_MW_W_KW
from utils.query_helpers import get_power_conversion_sql, should_exclude_metric
from database.connection_pool import get_pooled_connection
from analysis.energy import compute_energy_kwh_for_hostname


def get_hostname_from_nodeid(nodeid: int, database: str) -> str:
    """Get hostname from nodeid"""
    try:
        with get_pooled_connection(database, 'public') as client:
            query = f"SELECT hostname FROM public.nodes WHERE nodeid = {nodeid}"
            df = pd.read_sql_query(query, client.db_connection)
            if not df.empty:
                return df['hostname'].iloc[0]
            return None
    except Exception as e:
        click.echo(f"⚠️ Error getting hostname for nodeid {nodeid}: {e}")
        return None


def get_power_queries_for_nodeid(nodeid: int, database: str, start_time: str, end_time: str) -> Dict[str, str]:
    """Get power queries for a specific nodeid"""
    queries = {}
    
    try:
        # Connect to public schema to get power metrics list
        with get_pooled_connection(database, 'public') as client:
            df_power_metrics = pd.read_sql_query(POWER_METRICS_QUERY_UNIT_IN_MW_W_KW, client.db_connection)
            
            if df_power_metrics.empty:
                click.echo(f"⚠️ No power metrics found in {database} public schema")
                return {}
            
            # Get metric IDs
            power_metrics = df_power_metrics['metric_id'].str.lower().tolist()
            
            # Create queries for each power metric
            for metric in power_metrics:
                original_metric_id = df_power_metrics[df_power_metrics['metric_id'].str.lower() == metric]['metric_id'].iloc[0]
                
                # Create query using nodeid filter
                conversion_sql = get_power_conversion_sql(metric)
                
                modified_query = f"""
                SELECT 
                    m.timestamp,
                    n.hostname,
                    {conversion_sql} as value,
                    m.source,
                    m.fqdd,
                    md.units
                FROM idrac.{metric} m
                LEFT JOIN public.nodes n ON m.nodeid = n.nodeid
                LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('{original_metric_id}')
                WHERE m.nodeid = {nodeid} 
                AND m.timestamp BETWEEN '{start_time}' AND '{end_time}'
                ORDER BY m.timestamp ASC
                """
                
                queries[original_metric_id] = modified_query
        
        return queries
        
    except Exception as e:
        click.echo(f"❌ Error getting power queries: {e}")
        return {}


def run_power_queries(queries: Dict[str, str], database: str) -> Dict[str, pd.DataFrame]:
    """Run power queries and return results"""
    results = {}
    
    for query_name, query in queries.items():
        try:
            with get_pooled_connection(database, 'idrac') as client:
                df = pd.read_sql_query(query, client.db_connection)
                if not df.empty:
                    results[query_name] = df
        except Exception as e:
            click.echo(f"⚠️ Error running query {query_name}: {e}")
            continue
    
    return results


def create_power_plot(results: Dict[str, pd.DataFrame], output_dir: str, database: str, nodeid: int, hostname: str):
    """Create power consumption time series plot"""
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        # Set up plot style
        plt.style.use('default')
        rcParams['figure.figsize'] = (16, 10)
        rcParams['font.size'] = 22
        
        fig, ax = plt.subplots(figsize=(16, 10))
        
        # Define colors
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', 
                 '#9467bd', '#8c564b', '#e377c2', '#17becf']
        
        # Plot each power metric
        for i, (power_name, df) in enumerate(results.items()):
            if df.empty:
                continue
            
            if should_exclude_metric(power_name):
                continue
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                grouped_data = df.groupby('timestamp')['value'].mean().reset_index()
                
                ax.plot(grouped_data['timestamp'], grouped_data['value'], 
                       label=power_name, color=colors[i % len(colors)], 
                       linewidth=2, alpha=0.8)
        
        # Customize plot
        ax.set_xlabel('Time', fontsize=22, fontweight='bold')
        ax.set_ylabel('Power (Watts)', fontsize=22, fontweight='bold')
        title = f'Power Consumption Over Time - {database.upper()} Database - Node {nodeid}'
        if hostname:
            title += f' ({hostname})'
        ax.set_title(title, fontsize=26, fontweight='bold', pad=20)
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=12))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=14)
        plt.setp(ax.yaxis.get_majorticklabels(), fontsize=20)
        
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper right', fontsize=22)
        
        plt.tight_layout()
        
        # Save plot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(output_dir, f"power_consumption_{database}_node{nodeid}_{timestamp}.png")
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        click.echo(f"✓ Power plot saved: {filename}")
        
        # Also save as PDF
        pdf_filename = os.path.join(output_dir, f"power_consumption_{database}_node{nodeid}_{timestamp}.pdf")
        plt.savefig(pdf_filename, bbox_inches='tight')
        click.echo(f"✓ Power plot saved: {pdf_filename}")
        
        plt.close()
        
    except Exception as e:
        click.echo(f"❌ Error creating power plot: {e}")
        import traceback
        traceback.print_exc()


def create_energy_pie_chart(results: Dict[str, pd.DataFrame], output_dir: str, database: str, 
                           nodeid: int, hostname: str, start_time: str, end_time: str):
    """Create energy consumption pie chart"""
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        # Define metric categories (similar to H100/ZEN4 scripts)
        CPU_METRICS = ['CPUPower', 'PkgPwr', 'TotalCPUPower']
        MEMORY_METRICS = ['DRAMPwr', 'TotalMemoryPower']
        GPU_METRICS = ['PowerConsumption']
        FAN_METRICS = ['TotalFanPower']
        STORAGE_METRICS = ['TotalStoragePower']
        SYSTEM_METRICS = ['SystemInputPower', 'SystemOutputPower', 'SystemPowerConsumption', 'WattsReading']
        
        # Calculate energy for each metric
        energy_data = {}
        
        for metric_name, df in results.items():
            if df.empty or should_exclude_metric(metric_name):
                continue
            
            # Get unit
            unit = df['units'].iloc[0] if 'units' in df.columns and df['units'].notna().any() else 'W'
            
            # Get hostname from data
            data_hostname = df['hostname'].iloc[0] if 'hostname' in df.columns and not df['hostname'].isna().all() else hostname
            
            if data_hostname:
                energy = compute_energy_kwh_for_hostname(df, unit, data_hostname, start_time, end_time)
                if energy > 0:
                    energy_data[metric_name] = energy
        
        if not energy_data:
            click.echo("⚠️ No energy data to create pie chart")
            return
        
        # Group by component type using metric categories
        component_energy = {
            'CPU': 0.0,
            'MEMORY': 0.0,
            'GPU': 0.0,
            'FAN': 0.0,
            'STORAGE': 0.0,
            'Other': 0.0
        }
        
        system_output_energy = 0.0
        
        for metric, energy in energy_data.items():
            metric_upper = metric.upper()
            categorized = False
            
            # Check against defined categories
            if any(cpu_m in metric_upper for cpu_m in [m.upper() for m in CPU_METRICS]):
                component_energy['CPU'] += energy
                categorized = True
            elif any(mem_m in metric_upper for mem_m in [m.upper() for m in MEMORY_METRICS]):
                component_energy['MEMORY'] += energy
                categorized = True
            elif any(gpu_m in metric_upper for gpu_m in [m.upper() for m in GPU_METRICS]):
                component_energy['GPU'] += energy
                categorized = True
            elif any(fan_m in metric_upper for fan_m in [m.upper() for m in FAN_METRICS]):
                component_energy['FAN'] += energy
                categorized = True
            elif any(stor_m in metric_upper for stor_m in [m.upper() for m in STORAGE_METRICS]):
                component_energy['STORAGE'] += energy
                categorized = True
            elif 'SYSTEMOUTPUT' in metric_upper:
                system_output_energy = energy
                categorized = True
            
            if not categorized:
                component_energy['Other'] += energy
        
        # Calculate "Other" as difference between SystemOutput and sum of components
        component_sum = sum(v for k, v in component_energy.items() if k != 'Other')
        if system_output_energy > 0:
            other_val = max(system_output_energy - component_sum, 0.0)
            component_energy['Other'] = other_val
        
        # Remove zero components
        component_energy = {k: v for k, v in component_energy.items() if v > 0}
        
        if not component_energy:
            click.echo("⚠️ No energy data to create pie chart")
            return
        
        # Create pie chart
        plt.figure(figsize=(10, 7))
        labels = list(component_energy.keys())
        sizes = list(component_energy.values())
        
        if sum(sizes) == 0:
            sizes = [1 for _ in sizes]
        
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
        title = f'{database.upper()} Top-level Energy Share'
        if hostname:
            title += f' ({hostname})'
        elif nodeid:
            title += f' (Node {nodeid})'
        plt.title(title, fontsize=16, fontweight='bold')
        plt.axis('equal')
        
        # Save pie chart
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pie_filename = os.path.join(output_dir, f"energy_pie_{database}_node{nodeid}_{timestamp}.png")
        plt.savefig(pie_filename, bbox_inches='tight', dpi=300)
        click.echo(f"✓ Energy pie chart saved: {pie_filename}")
        
        plt.close()
        
    except Exception as e:
        click.echo(f"❌ Error creating energy pie chart: {e}")
        import traceback
        traceback.print_exc()


@click.command()
@click.option('--database', 
              type=click.Choice(['h100', 'zen4']), 
              default='h100',
              help='Database to query (h100 or zen4)')
@click.option('--nodeid', 
              type=int, 
              required=True,
              help='Node ID to query')
@click.option('--start-time', 
              required=True,
              help='Start time (YYYY-MM-DD HH:MM:SS)')
@click.option('--end-time', 
              required=True,
              help='End time (YYYY-MM-DD HH:MM:SS)')
@click.option('--output-dir', 
              default=None,
              help='Output directory (default: output/{database}/node_{nodeid})')
def visualize(database, nodeid, start_time, end_time, output_dir):
    """
    Query power consumption data by nodeid and generate power plot and energy pie chart.
    
    Examples:
        python -m src.cli visualize --database h100 --nodeid 4 --start-time "2025-01-01 00:00:00" --end-time "2025-01-01 23:59:59"
    """
    
    click.echo(f"📊 Generating power visualization for Node {nodeid}...")
    click.echo(f"📅 Time range: {start_time} to {end_time}")
    click.echo(f"🗄️  Database: {database}")
    click.echo()
    
    # Get hostname from nodeid
    hostname = get_hostname_from_nodeid(nodeid, database)
    if hostname:
        click.echo(f"🖥️  Hostname: {hostname}")
    else:
        click.echo(f"⚠️  Could not resolve hostname for nodeid {nodeid}, continuing...")
    
    click.echo()
    
    # Set output directory
    if not output_dir:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        output_dir = os.path.join(project_root, 'output', database, f'node_{nodeid}')
    
    # Get power queries
    click.echo("🔍 Building power queries...")
    queries = get_power_queries_for_nodeid(nodeid, database, start_time, end_time)
    click.echo(f"✓ Found {len(queries)} power metrics to query")
    
    if not queries:
        click.echo("❌ No power queries found")
        return
    
    # Run queries
    click.echo("📥 Querying power data...")
    results = run_power_queries(queries, database)
    click.echo(f"✓ Retrieved data for {len(results)} metrics")
    
    if not results:
        click.echo("❌ No power data retrieved")
        return
    
    # Create power plot
    click.echo("📈 Generating power consumption plot...")
    create_power_plot(results, output_dir, database, nodeid, hostname)
    
    # Create energy pie chart
    click.echo("🥧 Generating energy pie chart...")
    create_energy_pie_chart(results, output_dir, database, nodeid, hostname, start_time, end_time)
    
    click.echo()
    click.echo("✅ Visualization complete!")
    click.echo(f"📁 Output directory: {output_dir}")

