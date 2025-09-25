#!/usr/bin/env python3
"""
Report formatting module for REPACSS Power Measurement
Handles various output formats and templates
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ReportFormatter:
    """Handles report formatting for different output formats"""
    
    def __init__(self):
        self.output_dir = "output"
        self._ensure_output_dir()
    
    def _ensure_output_dir(self):
        """Ensure output directory exists"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def generate_custom_report(self, format: str, output_path: str, template: str = None):
        """
        Generate custom reports with various formats.
        
        Args:
            format: Output format ('excel', 'csv', 'json', 'html')
            output_path: Output file path
            template: Custom report template (optional)
        """
        print(f"📊 Generating {format.upper()} report: {output_path}")
        if template:
            print(f"📋 Template: {template}")
        print()
        
        try:
            if format == 'excel':
                self._generate_excel_report(output_path, template)
            elif format == 'csv':
                self._generate_csv_report(output_path, template)
            elif format == 'json':
                self._generate_json_report(output_path, template)
            elif format == 'html':
                self._generate_html_report(output_path, template)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            print(f"✅ Custom report generated successfully: {output_path}")
            
        except Exception as e:
            print(f"❌ Error generating custom report: {e}")
            raise
    
    def _generate_excel_report(self, output_path: str, template: str = None):
        """Generate Excel report"""
        from .excel import ExcelReporter
        
        reporter = ExcelReporter()
        reporter.generate_report(['h100', 'zen4', 'infra'], output_path)
    
    def _generate_csv_report(self, output_path: str, template: str = None):
        """Generate CSV report"""
        # This would collect data and export to CSV
        # For now, create a placeholder
        data = {
            'timestamp': [datetime.now()],
            'message': ['CSV report generation not yet implemented'],
            'format': ['csv']
        }
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
    
    def _generate_json_report(self, output_path: str, template: str = None):
        """Generate JSON report"""
        # This would collect data and export to JSON
        # For now, create a placeholder
        data = {
            'timestamp': datetime.now().isoformat(),
            'message': 'JSON report generation not yet implemented',
            'format': 'json'
        }
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _generate_html_report(self, output_path: str, template: str = None):
        """Generate HTML report"""
        # This would collect data and export to HTML
        # For now, create a placeholder
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>REPACSS Power Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .content {{ margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>REPACSS Power Measurement Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            <div class="content">
                <p>HTML report generation not yet fully implemented.</p>
                <p>This is a placeholder for future HTML reporting functionality.</p>
            </div>
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html_content)
    
    def format_power_summary(self, data: Dict[str, Any]) -> str:
        """
        Format power analysis summary as text.
        
        Args:
            data: Power analysis data
        
        Returns:
            Formatted summary string
        """
        if not data:
            return "No data available"
        
        summary_lines = []
        summary_lines.append("📊 Power Analysis Summary")
        summary_lines.append("=" * 50)
        
        if 'data' in data:
            # Single node analysis
            df = data['data']
            summary = data.get('summary', {})
            
            summary_lines.append(f"🖥️  Hostname: {data.get('hostname', 'Unknown')}")
            summary_lines.append(f"📅 Time Range: {data.get('start_time', 'Unknown')} to {data.get('end_time', 'Unknown')}")
            summary_lines.append(f"📊 Records: {summary.get('total_records', 0)}")
            summary_lines.append(f"⏱️  Duration: {summary.get('time_range_hours', 0):.2f} hours")
            
            if 'power_w' in summary:
                summary_lines.append(f"⚡ Average Power: {summary.get('avg_power_w', 0):.2f} W")
                summary_lines.append(f"⚡ Max Power: {summary.get('max_power_w', 0):.2f} W")
                summary_lines.append(f"⚡ Min Power: {summary.get('min_power_w', 0):.2f} W")
                summary_lines.append(f"🔋 Total Energy: {summary.get('total_energy_kwh', 0):.4f} kWh")
        else:
            # Multi-node analysis
            for hostname, node_data in data.items():
                if isinstance(node_data, dict) and 'summary' in node_data:
                    summary = node_data['summary']
                    summary_lines.append(f"\n🖥️  {hostname}:")
                    summary_lines.append(f"  📊 Records: {summary.get('total_records', 0)}")
                    if 'power_w' in summary:
                        summary_lines.append(f"  ⚡ Avg Power: {summary.get('avg_power_w', 0):.2f} W")
                        summary_lines.append(f"  🔋 Total Energy: {summary.get('total_energy_kwh', 0):.4f} kWh")
        
        return "\n".join(summary_lines)
    
    def format_energy_summary(self, energy_results: Dict[str, float]) -> str:
        """
        Format energy calculation summary as text.
        
        Args:
            energy_results: Energy calculation results
        
        Returns:
            Formatted summary string
        """
        if not energy_results:
            return "No energy data available"
        
        summary_lines = []
        summary_lines.append("⚡ Energy Consumption Summary")
        summary_lines.append("=" * 50)
        
        total_energy = 0.0
        for metric, energy_kwh in energy_results.items():
            summary_lines.append(f"{metric:30} {energy_kwh:10.4f} kWh")
            total_energy += energy_kwh
        
        summary_lines.append("-" * 50)
        summary_lines.append(f"{'Total Energy':30} {total_energy:10.4f} kWh")
        summary_lines.append(f"{'Total Energy':30} {total_energy * 1000:10.2f} Wh")
        
        return "\n".join(summary_lines)
    
    def create_summary_table(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Create summary table from power analysis data.
        
        Args:
            data: Power analysis data
        
        Returns:
            Summary DataFrame
        """
        summary_rows = []
        
        if 'data' in data:
            # Single node analysis
            summary = data.get('summary', {})
            summary_rows.append({
                'hostname': data.get('hostname', 'Unknown'),
                'records': summary.get('total_records', 0),
                'duration_hours': summary.get('time_range_hours', 0),
                'avg_power_w': summary.get('avg_power_w', 0),
                'max_power_w': summary.get('max_power_w', 0),
                'min_power_w': summary.get('min_power_w', 0),
                'total_energy_kwh': summary.get('total_energy_kwh', 0)
            })
        else:
            # Multi-node analysis
            for hostname, node_data in data.items():
                if isinstance(node_data, dict) and 'summary' in node_data:
                    summary = node_data['summary']
                    summary_rows.append({
                        'hostname': hostname,
                        'records': summary.get('total_records', 0),
                        'duration_hours': summary.get('time_range_hours', 0),
                        'avg_power_w': summary.get('avg_power_w', 0),
                        'max_power_w': summary.get('max_power_w', 0),
                        'min_power_w': summary.get('min_power_w', 0),
                        'total_energy_kwh': summary.get('total_energy_kwh', 0)
                    })
        
        return pd.DataFrame(summary_rows)
    
    def export_summary_to_csv(self, data: Dict[str, Any], output_path: str):
        """Export summary table to CSV"""
        summary_df = self.create_summary_table(data)
        summary_df.to_csv(output_path, index=False)
        print(f"✅ Summary exported to CSV: {output_path}")
    
    def export_summary_to_json(self, data: Dict[str, Any], output_path: str):
        """Export summary to JSON"""
        summary_df = self.create_summary_table(data)
        summary_dict = summary_df.to_dict('records')
        
        with open(output_path, 'w') as f:
            json.dump(summary_dict, f, indent=2, default=str)
        
        print(f"✅ Summary exported to JSON: {output_path}")
