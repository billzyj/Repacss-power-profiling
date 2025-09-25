#!/usr/bin/env python3
"""
Reporting CLI commands
"""

import click
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from reporting.excel import ExcelReporter
from reporting.formats import ReportFormatter


@click.command()
@click.option('--output', 
              help='Output file path (default: auto-generated)')
@click.option('--databases', 
              multiple=True,
              type=click.Choice(['h100', 'zen4', 'infra']),
              default=['h100', 'zen4', 'infra'],
              help='Databases to include in report')
@click.option('--sheets', 
              multiple=True,
              help='Specific sheets to include (optional)')
def excel(output, databases, sheets):
    """
    Generate comprehensive Excel power metrics report.
    
    Examples:
        # Generate report with all databases
        python -m src.cli report excel
        
        # Generate report for specific databases
        python -m src.cli report excel --databases h100 zen4
        
        # Generate report with custom output path
        python -m src.cli report excel --output custom_report.xlsx
    """
    
    if not output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output = f"power_metrics_report_{timestamp}.xlsx"
    
    click.echo(f"📊 Generating Excel report: {output}")
    click.echo(f"🗄️  Databases: {', '.join(databases)}")
    if sheets:
        click.echo(f"📋 Sheets: {', '.join(sheets)}")
    click.echo()
    
    try:
        # Initialize reporter
        reporter = ExcelReporter()
        
        # Generate report
        reporter.generate_report(
            databases=list(databases),
            output_path=output,
            specific_sheets=list(sheets) if sheets else None
        )
        
        click.echo(f"✅ Excel report generated successfully: {output}")
        
    except Exception as e:
        click.echo(f"❌ Error generating Excel report: {e}")
        sys.exit(1)


@click.command()
@click.option('--rack', 
              type=int,
              help='Specific rack number (91-97)')
@click.option('--output', 
              help='Output directory (default: output/rack/)')
@click.option('--start-time', 
              help='Start time (YYYY-MM-DD HH:MM:SS)')
@click.option('--end-time', 
              help='End time (YYYY-MM-DD HH:MM:SS)')
def rack_report(rack, output, start_time, end_time):
    """
    Generate rack-level power analysis reports.
    
    Examples:
        # Analyze all racks
        python -m src.cli report rack
        
        # Analyze specific rack
        python -m src.cli report rack --rack 97
        
        # Analyze with custom time range
        python -m src.cli report rack --start-time "2025-01-01 00:00:00" --end-time "2025-01-02 00:00:00"
    """
    
    if not output:
        output = "output/rack/"
    
    click.echo(f"🏗️  Generating rack analysis report...")
    if rack:
        click.echo(f"🔧 Rack: {rack}")
    else:
        click.echo(f"🔧 Racks: All (91-97)")
    click.echo(f"📁 Output: {output}")
    if start_time and end_time:
        click.echo(f"📅 Time range: {start_time} to {end_time}")
    click.echo()
    
    try:
        # Initialize reporter
        reporter = ExcelReporter()
        
        # Generate rack analysis
        reporter.generate_rack_analysis(
            rack_number=rack,
            output_dir=output,
            start_time=start_time,
            end_time=end_time
        )
        
        click.echo(f"✅ Rack analysis report generated successfully")
        
    except Exception as e:
        click.echo(f"❌ Error generating rack analysis: {e}")
        sys.exit(1)


@click.command()
@click.option('--format', 
              type=click.Choice(['excel', 'csv', 'json', 'html']), 
              default='excel',
              help='Output format')
@click.option('--output', 
              help='Output file path')
@click.option('--template', 
              help='Custom report template')
def custom(format, output, template):
    """
    Generate custom reports with various formats.
    
    Examples:
        # Generate HTML report
        python -m src.cli report custom --format html --output report.html
        
        # Generate CSV report
        python -m src.cli report custom --format csv --output data.csv
    """
    
    if not output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output = f"custom_report_{timestamp}.{format}"
    
    click.echo(f"📊 Generating {format.upper()} report: {output}")
    if template:
        click.echo(f"📋 Template: {template}")
    click.echo()
    
    try:
        # Initialize formatter
        formatter = ReportFormatter()
        
        # Generate custom report
        formatter.generate_custom_report(
            format=format,
            output_path=output,
            template=template
        )
        
        click.echo(f"✅ Custom report generated successfully: {output}")
        
    except Exception as e:
        click.echo(f"❌ Error generating custom report: {e}")
        sys.exit(1)
