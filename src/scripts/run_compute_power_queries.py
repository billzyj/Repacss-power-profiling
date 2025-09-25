#!/usr/bin/env python3
"""
Unified Compute Power Query Runner
Handles both H100 and ZEN4 power queries with consolidated functionality
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
import argparse

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.manager import QueryManager
from analysis.power import PowerAnalyzer
from analysis.energy import EnergyCalculator
from reporting.excel import ExcelReporter
from database.connection_pool import close_all_pools


class ComputePowerQueryRunner:
    """Unified runner for compute power queries (H100 and ZEN4)"""
    
    def __init__(self, database: str):
        self.database = database
        self.query_manager = QueryManager(database)
        self.power_analyzer = PowerAnalyzer(database)
        self.energy_calculator = EnergyCalculator(database)
        self.reporter = ExcelReporter()
        
        # Define metric constants
        self.CPU_METRICS = ['CPUPower', 'PkgPwr', 'TotalCPUPower']
        self.MEMORY_METRICS = ['DRAMPwr', 'TotalMemoryPower']
        self.FAN_METRICS = ['TotalFanPower']
        self.STORAGE_METRICS = ['TotalStoragePower']
        self.SYSTEM_METRICS = ['SystemInputPower', 'SystemOutputPower', 'SystemPowerConsumption', 'WattsReading']
        
        # Add GPU metrics for H100
        if database == 'h100':
            self.GPU_METRICS = ['PowerConsumption']
        else:
            self.GPU_METRICS = []
    
    def get_power_metrics(self, hostname: str = None, start_time: datetime = None, 
                         end_time: datetime = None, limit: int = 1000) -> Dict[str, pd.DataFrame]:
        """
        Get power metrics for compute nodes.
        
        Args:
            hostname: Specific hostname to analyze (optional)
            start_time: Start timestamp (optional)
            end_time: End timestamp (optional)
            limit: Maximum number of records (default: 1000)
        
        Returns:
            Dictionary with metric results
        """
        print(f"📊 Collecting {self.database.upper()} power metrics...")
        
        try:
            # Get power metrics definition
            power_metrics_df = self.query_manager.get_power_metrics_definition()
            
            if power_metrics_df.empty:
                print("  - No power metrics found")
                return {}
            
            # Get metric IDs
            power_metrics = power_metrics_df['metric_id'].str.lower().tolist()
            print(f"  - Found {len(power_metrics)} power metrics to process")
            
            results = {}
            
            # Process each power metric
            for metric in power_metrics:
                try:
                    # Get query for this metric
                    if hostname:
                        query = self.query_manager.get_power_metrics(hostname, start_time, end_time, limit)
                        metric_data = query[query['metric'] == metric] if not query.empty else pd.DataFrame()
                    else:
                        # Get data for all nodes
                        metric_data = self.query_manager.get_power_metrics(
                            hostname=None, start_time=start_time, end_time=end_time, limit=limit
                        )
                        metric_data = metric_data[metric_data['metric'] == metric] if not metric_data.empty else pd.DataFrame()
                    
                    if not metric_data.empty:
                        # Convert timestamp to timezone-unaware for Excel compatibility
                        if 'timestamp' in metric_data.columns:
                            metric_data['timestamp'] = metric_data['timestamp'].dt.tz_localize(None)
                        
                        # Use the original metric ID (with proper case) as key
                        original_metric_id = power_metrics_df[power_metrics_df['metric_id'].str.lower() == metric]['metric_id'].iloc[0]
                        results[f'{self.database.upper()}_{original_metric_id}'] = metric_data
                        print(f"  - {original_metric_id}: {len(metric_data)} rows")
                    else:
                        print(f"  - {metric}: No data found")
                
                except Exception as e:
                    print(f"  - {metric}: Error - {e}")
                    continue
            
            return results
            
        except Exception as e:
            print(f"❌ Error collecting power metrics: {e}")
            return {}
    
    def analyze_power_consumption(self, hostname: str, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Analyze power consumption for a specific node and time range.
        
        Args:
            hostname: Node hostname
            start_time: Start timestamp
            end_time: End timestamp
        
        Returns:
            Power analysis results
        """
        print(f"🔍 Analyzing power consumption for {hostname}...")
        print(f"📅 Time range: {start_time} to {end_time}")
        
        try:
            # Run power analysis
            results = self.power_analyzer.analyze_power(hostname, start_time, end_time)
            
            if results:
                print("✅ Power analysis completed")
                return results
            else:
                print("❌ No data found for analysis")
                return {}
                
        except Exception as e:
            print(f"❌ Error in power analysis: {e}")
            return {}
    
    def calculate_energy_consumption(self, hostname: str, start_time: datetime, end_time: datetime) -> Dict[str, float]:
        """
        Calculate energy consumption for a specific node and time range.
        
        Args:
            hostname: Node hostname
            start_time: Start timestamp
            end_time: End timestamp
        
        Returns:
            Energy consumption results
        """
        print(f"⚡ Calculating energy consumption for {hostname}...")
        
        try:
            # Calculate energy
            energy_results = self.energy_calculator.calculate_energy(hostname, start_time, end_time)
            
            if energy_results:
                print("✅ Energy calculation completed")
                return energy_results
            else:
                print("❌ No data found for energy calculation")
                return {}
                
        except Exception as e:
            print(f"❌ Error in energy calculation: {e}")
            return {}
    
    def generate_excel_report(self, results: Dict[str, pd.DataFrame], output_path: str = None) -> str:
        """
        Generate Excel report from power metrics results.
        
        Args:
            results: Power metrics results
            output_path: Output file path (optional)
        
        Returns:
            Path to generated Excel file
        """
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"output/{self.database}_power_analysis_{timestamp}.xlsx"
        
        print(f"📊 Generating Excel report: {output_path}")
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet_name, df in results.items():
                    if not df.empty:
                        # Limit sheet name length
                        safe_sheet_name = sheet_name[:31]
                        df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                        print(f"  - {safe_sheet_name}: {len(df)} rows")
                
                if not results:
                    # Create empty sheet if no data
                    empty_df = pd.DataFrame({'message': ['No data available']})
                    empty_df.to_excel(writer, sheet_name='No_Data', index=False)
            
            print(f"✅ Excel report generated: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"❌ Error generating Excel report: {e}")
            raise
    
    def run_comprehensive_analysis(self, hostname: str = None, hours: int = 24, 
                                 output_path: str = None) -> Dict[str, Any]:
        """
        Run comprehensive power analysis including metrics, energy, and reporting.
        
        Args:
            hostname: Specific hostname to analyze (optional)
            hours: Number of hours to analyze (default: 24)
            output_path: Output file path (optional)
        
        Returns:
            Comprehensive analysis results
        """
        print(f"🚀 Running comprehensive {self.database.upper()} power analysis...")
        print("=" * 60)
        
        # Set time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        print(f"📅 Time range: {start_time} to {end_time}")
        if hostname:
            print(f"🖥️  Hostname: {hostname}")
        print()
        
        results = {
            'database': self.database,
            'start_time': start_time,
            'end_time': end_time,
            'hostname': hostname,
            'power_metrics': {},
            'power_analysis': {},
            'energy_calculation': {},
            'excel_report': None
        }
        
        try:
            # 1. Get power metrics
            print("📊 Step 1: Collecting power metrics...")
            power_metrics = self.get_power_metrics(hostname, start_time, end_time)
            results['power_metrics'] = power_metrics
            print(f"✅ Collected {len(power_metrics)} metric types")
            print()
            
            # 2. Power analysis (if hostname specified)
            if hostname:
                print("🔍 Step 2: Running power analysis...")
                power_analysis = self.analyze_power_consumption(hostname, start_time, end_time)
                results['power_analysis'] = power_analysis
                print("✅ Power analysis completed")
                print()
                
                # 3. Energy calculation
                print("⚡ Step 3: Calculating energy consumption...")
                energy_calculation = self.calculate_energy_consumption(hostname, start_time, end_time)
                results['energy_calculation'] = energy_calculation
                print("✅ Energy calculation completed")
                print()
            
            # 4. Generate Excel report
            print("📊 Step 4: Generating Excel report...")
            excel_path = self.generate_excel_report(power_metrics, output_path)
            results['excel_report'] = excel_path
            print("✅ Excel report generated")
            print()
            
            print("🎉 Comprehensive analysis completed successfully!")
            return results
            
        except Exception as e:
            print(f"❌ Error in comprehensive analysis: {e}")
            return results
        finally:
            close_all_pools()


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='REPACSS Compute Power Query Runner')
    parser.add_argument('database', choices=['h100', 'zen4'], help='Database to analyze')
    parser.add_argument('--hostname', help='Specific hostname to analyze')
    parser.add_argument('--hours', type=int, default=24, help='Number of hours to analyze')
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--analysis-only', action='store_true', help='Run only power analysis (no metrics collection)')
    parser.add_argument('--energy-only', action='store_true', help='Run only energy calculation')
    
    args = parser.parse_args()
    
    # Initialize runner
    runner = ComputePowerQueryRunner(args.database)
    
    try:
        if args.analysis_only:
            # Run only power analysis
            if not args.hostname:
                print("❌ Hostname required for power analysis")
                return
            
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=args.hours)
            
            results = runner.analyze_power_consumption(args.hostname, start_time, end_time)
            if results:
                runner.power_analyzer.display_summary(results)
        
        elif args.energy_only:
            # Run only energy calculation
            if not args.hostname:
                print("❌ Hostname required for energy calculation")
                return
            
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=args.hours)
            
            results = runner.calculate_energy_consumption(args.hostname, start_time, end_time)
            if results:
                runner.energy_calculator.display_energy_summary(results)
        
        else:
            # Run comprehensive analysis
            results = runner.run_comprehensive_analysis(
                hostname=args.hostname,
                hours=args.hours,
                output_path=args.output
            )
            
            # Display summary
            if results.get('power_analysis'):
                runner.power_analyzer.display_summary(results['power_analysis'])
            
            if results.get('energy_calculation'):
                runner.energy_calculator.display_energy_summary(results['energy_calculation'])
    
    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        close_all_pools()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
