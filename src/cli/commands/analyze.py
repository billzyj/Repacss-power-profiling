#!/usr/bin/env python3
"""
Power analysis CLI commands
"""

import click
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.power_service import PowerAnalysisService


@click.command()
@click.option('--database', 
              type=click.Choice(['h100', 'zen4', 'infra']), 
              default='h100',
              help='Database to analyze')
@click.option('--hostname', 
              help='Specific hostname to analyze (optional)')
@click.option('--start-time', 
              help='Start time (YYYY-MM-DD HH:MM:SS)')
@click.option('--end-time', 
              help='End time (YYYY-MM-DD HH:MM:SS)')
@click.option('--hours', 
              type=int, 
              default=1,
              help='Number of hours to analyze (if no start/end time specified)')
@click.option('--output', 
              help='Output file path (optional)')
@click.option('--format', 
              type=click.Choice(['excel', 'csv', 'json']), 
              default='excel',
              help='Output format')
def analyze(database, hostname, start_time, end_time, hours, output, format):
    """
    Analyze power consumption data from REPACSS cluster.
    
    Examples:
        # Analyze H100 nodes for last hour
        python -m src.cli analyze --database h100
        
        # Analyze specific node for custom time range
        python -m src.cli analyze --database h100 --hostname rpg-93-1 --start-time "2025-01-01 00:00:00" --end-time "2025-01-01 23:59:59"
        
        # Analyze ZEN4 nodes for 6 hours
        python -m src.cli analyze --database zen4 --hours 6
    """
    
    # Parse time range
    if start_time and end_time:
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    elif start_time:
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = start_dt + timedelta(hours=hours)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=hours)
    
    click.echo(f"🔍 Analyzing {database} database...")
    click.echo(f"📅 Time range: {start_dt} to {end_dt}")
    if hostname:
        click.echo(f"🖥️  Hostname: {hostname}")
    click.echo()
    
    try:
        # Initialize service
        service = PowerAnalysisService(database)
        
        if hostname:
            # Single node analysis
            results = service.analyze_node_power(hostname, start_dt, end_dt)
        else:
            # System overview
            results = service.get_system_overview(hours)
        
        if 'error' in results:
            click.echo(f"❌ Analysis failed: {results['error']}")
            return
        
        # Display results
        _display_analysis_results(results, output, format)
            
    except Exception as e:
        click.echo(f"❌ Error during analysis: {e}")
        sys.exit(1)


@click.command()
@click.option('--database', 
              type=click.Choice(['h100', 'zen4', 'infra']), 
              default='h100',
              help='Database to analyze')
@click.option('--hostname', 
              help='Specific hostname to analyze (optional)')
@click.option('--start-time', 
              help='Start time (YYYY-MM-DD HH:MM:SS)')
@click.option('--end-time', 
              help='End time (YYYY-MM-DD HH:MM:SS)')
@click.option('--hours', 
              type=int, 
              default=24,
              help='Number of hours to analyze (if no start/end time specified)')
def energy(database, hostname, start_time, end_time, hours):
    """
    Calculate energy consumption for power analysis.
    
    Examples:
        # Calculate energy for H100 nodes for last 24 hours
        python -m src.cli energy --database h100
        
        # Calculate energy for specific node
        python -m src.cli energy --database h100 --hostname rpg-93-1 --hours 12
    """
    
    # Parse time range
    if start_time and end_time:
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    elif start_time:
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = start_dt + timedelta(hours=hours)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=hours)
    
    click.echo(f"⚡ Calculating energy consumption for {database} database...")
    click.echo(f"📅 Time range: {start_dt} to {end_dt}")
    if hostname:
        click.echo(f"🖥️  Hostname: {hostname}")
    click.echo()
    
    try:
        # Initialize service
        service = PowerAnalysisService(database)
        
        # Calculate energy
        results = service.analyze_node_power(hostname, start_dt, end_dt)
        
        if 'error' in results:
            click.echo(f"❌ Energy calculation failed: {results['error']}")
            return
        
        # Display energy results
        if 'energy_consumption' in results:
            energy_results = results['energy_consumption']
            click.echo("⚡ Energy Consumption Summary")
            click.echo("=" * 50)
            
            total_energy = 0.0
            for metric, energy_kwh in energy_results.items():
                click.echo(f"{metric:30} {energy_kwh:10.4f} kWh")
                total_energy += energy_kwh
            
            click.echo("-" * 50)
            click.echo(f"{'Total Energy':30} {total_energy:10.4f} kWh")
            click.echo(f"{'Total Energy':30} {total_energy * 1000:10.2f} Wh")
        else:
            click.echo("❌ No energy data found")
        
    except Exception as e:
        click.echo(f"❌ Error during energy calculation: {e}")
        sys.exit(1)


@click.command()
@click.option('--rack', 
              type=int,
              help='Rack number (91-97)')
@click.option('--start-time', 
              help='Start time (YYYY-MM-DD HH:MM:SS)')
@click.option('--end-time', 
              help='End time (YYYY-MM-DD HH:MM:SS)')
@click.option('--hours', 
              type=int, 
              default=24,
              help='Number of hours to analyze (if no start/end time specified)')
def rack(rack, start_time, end_time, hours):
    """
    Analyze power consumption for an entire rack.
    
    Examples:
        # Analyze rack 97 for last 24 hours
        python -m src.cli analyze rack --rack 97
        
        # Analyze rack 91 with custom time range
        python -m src.cli analyze rack --rack 91 --start-time "2025-01-01 00:00:00" --end-time "2025-01-02 00:00:00"
    """
    
    if not rack:
        click.echo("❌ Rack number is required")
        return
    
    # Parse time range
    if start_time and end_time:
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    elif start_time:
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = start_dt + timedelta(hours=hours)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=hours)
    
    click.echo(f"🏗️  Analyzing Rack {rack}...")
    click.echo(f"📅 Time range: {start_dt} to {end_dt}")
    click.echo()
    
    try:
        # Initialize service (use infra database for rack analysis)
        service = PowerAnalysisService('infra')
        
        # Analyze rack
        results = service.analyze_rack_power(rack, start_dt, end_dt)
        
        if 'error' in results:
            click.echo(f"❌ Rack analysis failed: {results['error']}")
            return
        
        # Display results
        click.echo("🏗️  Rack Analysis Summary")
        click.echo("=" * 50)
        click.echo(f"Rack: {rack}")
        click.echo(f"Nodes: {len(results.get('nodes', []))}")
        click.echo(f"Nodes Analyzed: {len(results.get('node_analyses', {}))}")
        click.echo(f"Total Energy: {results.get('total_energy_kwh', 0):.4f} kWh")
        
        if 'summary' in results:
            summary = results['summary']
            click.echo(f"Status: {summary.get('status', 'unknown')}")
            click.echo(f"Average Energy per Node: {summary.get('avg_energy_per_node', 0):.4f} kWh")
        
    except Exception as e:
        click.echo(f"❌ Error during rack analysis: {e}")
        sys.exit(1)


def _display_analysis_results(results: Dict[str, Any], output: str = None, output_format: str = 'excel'):
    """Display analysis results in the specified format"""
    if not results:
        click.echo("❌ No data found")
        return
    
    # Display summary
    if 'summary' in results:
        summary = results['summary']
        click.echo("📊 Analysis Summary")
        click.echo("=" * 50)
        click.echo(f"Status: {summary.get('status', 'unknown')}")
        
        if 'total_records' in summary:
            click.echo(f"Records: {summary['total_records']}")
        if 'avg_power_w' in summary:
            click.echo(f"Average Power: {summary['avg_power_w']:.2f} W")
        if 'total_energy_kwh' in summary:
            click.echo(f"Total Energy: {summary['total_energy_kwh']:.4f} kWh")
    
    # Export if requested
    if output:
        try:
            if output_format == 'excel':
                _export_to_excel(results, output)
            elif output_format == 'csv':
                _export_to_csv(results, output)
            elif output_format == 'json':
                _export_to_json(results, output)
            click.echo(f"✅ Results exported to {output}")
        except Exception as e:
            click.echo(f"❌ Export failed: {e}")


def _export_to_excel(results: Dict[str, Any], output_path: str):
    """Export results to Excel"""
    import pandas as pd
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet
        if 'summary' in results:
            summary_df = pd.DataFrame([results['summary']])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Data sheets if available
        if 'power_analysis' in results and 'data' in results['power_analysis']:
            results['power_analysis']['data'].to_excel(writer, sheet_name='Power_Data', index=False)


def _export_to_csv(results: Dict[str, Any], output_path: str):
    """Export results to CSV"""
    import pandas as pd
    
    if 'power_analysis' in results and 'data' in results['power_analysis']:
        results['power_analysis']['data'].to_csv(output_path, index=False)
    else:
        # Create summary CSV
        summary_df = pd.DataFrame([results.get('summary', {})])
        summary_df.to_csv(output_path, index=False)


def _export_to_json(results: Dict[str, Any], output_path: str):
    """Export results to JSON"""
    import json
    
    # Convert datetime objects to strings
    def convert_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj
    
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2, default=convert_datetime)
