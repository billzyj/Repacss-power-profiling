#!/usr/bin/env python3
"""
Excel reporting module for REPACSS Power Measurement
Handles Excel file generation and formatting
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database import connect_to_database, disconnect_all
from queries.compute.public import *
from queries.infra.public import *


class ExcelReporter:
    """Handles Excel report generation for power metrics"""
    
    def __init__(self):
        self.output_dir = "output"
        self._ensure_output_dir()
    
    def _ensure_output_dir(self):
        """Ensure output directory exists"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def generate_report(self, databases: List[str], output_path: str = None, 
                       specific_sheets: List[str] = None) -> str:
        """
        Generate comprehensive Excel power metrics report.
        
        Args:
            databases: List of databases to include
            output_path: Output file path (optional)
            specific_sheets: Specific sheets to include (optional)
        
        Returns:
            Path to generated Excel file
        """
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(self.output_dir, f"power_metrics_report_{timestamp}.xlsx")
        
        print(f"📊 Generating Excel report: {output_path}")
        print(f"🗄️  Databases: {', '.join(databases)}")
        if specific_sheets:
            print(f"📋 Sheets: {', '.join(specific_sheets)}")
        print()
        
        try:
            # Collect data from all databases
            all_data = {}
            
            for database in databases:
                print(f"📊 Collecting {database} metrics...")
                db_data = self._collect_database_metrics(database)
                all_data.update(db_data)
            
            # Generate Excel file
            self._create_excel_file(all_data, output_path, specific_sheets)
            
            print(f"✅ Excel report generated successfully: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"❌ Error generating Excel report: {e}")
            raise
        finally:
            disconnect_all()
    
    def generate_rack_analysis(self, rack_number: int = None, output_dir: str = None, 
                             start_time: str = None, end_time: str = None) -> List[str]:
        """
        Generate rack-level power analysis reports.
        
        Args:
            rack_number: Specific rack number (91-97) or None for all racks
            output_dir: Output directory (optional)
            start_time: Start time for analysis (optional)
            end_time: End time for analysis (optional)
        
        Returns:
            List of generated file paths
        """
        if not output_dir:
            output_dir = os.path.join(self.output_dir, "rack")
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        print(f"🏗️  Generating rack analysis report...")
        if rack_number:
            print(f"🔧 Rack: {rack_number}")
        else:
            print(f"🔧 Racks: All (91-97)")
        print(f"📁 Output: {output_dir}")
        if start_time and end_time:
            print(f"📅 Time range: {start_time} to {end_time}")
        print()
        
        try:
            # Import rack analysis functionality
            from analysis.power import PowerAnalyzer
            from analysis.power import (
                RACK_91_COMPUTE_NODES, RACK_91_PD_NODES,
                RACK_92_COMPUTE_NODES, RACK_92_PD_NODES,
                RACK_93_COMPUTE_NODES, RACK_93_PD_NODES,
                RACK_94_COMPUTE_NODES, RACK_94_PD_NODES,
                RACK_95_COMPUTE_NODES, RACK_95_PD_NODES,
                RACK_96_COMPUTE_NODES, RACK_96_PD_NODES,
                RACK_97_COMPUTE_NODES, RACK_97_PDU_NODES
            )
            
            # Rack configuration
            rack_configs = {
                91: {'compute_nodes': RACK_91_COMPUTE_NODES, 'pdu_nodes': RACK_91_PD_NODES},
                92: {'compute_nodes': RACK_92_COMPUTE_NODES, 'pdu_nodes': RACK_92_PD_NODES},
                93: {'compute_nodes': RACK_93_COMPUTE_NODES, 'pdu_nodes': RACK_93_PD_NODES},
                94: {'compute_nodes': RACK_94_COMPUTE_NODES, 'pdu_nodes': RACK_94_PD_NODES},
                95: {'compute_nodes': RACK_95_COMPUTE_NODES, 'pdu_nodes': RACK_95_PD_NODES},
                96: {'compute_nodes': RACK_96_COMPUTE_NODES, 'pdu_nodes': RACK_96_PD_NODES},
                97: {'compute_nodes': RACK_97_COMPUTE_NODES, 'pdu_nodes': RACK_97_PDU_NODES}
            }
            
            generated_files = []
            
            # Analyze specified rack or all racks
            racks_to_analyze = [rack_number] if rack_number else list(rack_configs.keys())
            
            for rack_num in racks_to_analyze:
                if rack_num not in rack_configs:
                    print(f"⚠️  Rack {rack_num} not found in configuration")
                    continue
                
                print(f"🔧 Analyzing Rack {rack_num}...")
                
                config = rack_configs[rack_num]
                
                # Set time range
                if start_time and end_time:
                    start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                    end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                else:
                    end_dt = datetime.now()
                    start_dt = end_dt - pd.Timedelta(days=2)
                
                # Analyze compute nodes
                compute_results = multi_node_power_analysis(
                    config['compute_nodes'], 
                    start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    end_dt.strftime('%Y-%m-%d %H:%M:%S')
                )
                
                # Analyze PDU nodes
                pdu_results = multi_node_power_analysis(
                    config['pdu_nodes'], 
                    start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    end_dt.strftime('%Y-%m-%d %H:%M:%S')
                )
                
                # Generate Excel file for this rack
                output_file = os.path.join(output_dir, f"rack{rack_num}_power_analysis_{start_dt.strftime('%Y%m%d_%H%M%S')}.xlsx")
                self._create_rack_excel_file(rack_num, compute_results, pdu_results, output_file)
                generated_files.append(output_file)
                
                print(f"✅ Rack {rack_num} analysis completed: {output_file}")
            
            print(f"✅ Rack analysis completed: {len(generated_files)} files generated")
            return generated_files
            
        except Exception as e:
            print(f"❌ Error generating rack analysis: {e}")
            raise
        finally:
            disconnect_all()
    
    def export_analysis_results(self, results: Dict[str, Any], output_path: str):
        """
        Export power analysis results to Excel file.
        
        Args:
            results: Analysis results dictionary
            output_path: Output file path
        """
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                if 'data' in results:
                    # Single node analysis
                    results['data'].to_excel(writer, sheet_name='Power_Analysis', index=False)
                else:
                    # Multi-node analysis
                    for hostname, node_results in results.items():
                        if isinstance(node_results, dict) and 'data' in node_results:
                            sheet_name = f"Power_{hostname}"[:31]  # Excel sheet name limit
                            node_results['data'].to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Analysis results exported to: {output_path}")
            
        except Exception as e:
            print(f"❌ Error exporting analysis results: {e}")
            raise
    
    def _collect_database_metrics(self, database: str) -> Dict[str, pd.DataFrame]:
        """Collect metrics from a specific database"""
        try:
            # Connect to database
            client = connect_to_database(database, 'public')
            if not client:
                print(f"❌ Failed to connect to {database} database")
                return {}
            
            results = {}
            
            if database in ['h100', 'zen4']:
                # Compute database metrics
                results.update(self._collect_compute_metrics(client))
            elif database == 'infra':
                # Infrastructure database metrics
                results.update(self._collect_infra_metrics(client))
            
            return results
            
        except Exception as e:
            print(f"❌ Error collecting {database} metrics: {e}")
            return {}
    
    def _collect_compute_metrics(self, client) -> Dict[str, pd.DataFrame]:
        """Collect compute database metrics"""
        results = {}
        
        try:
            # Get all metrics
            df_all_metrics = pd.read_sql_query(ALL_METRICS, client.db_connection)
            results['Compute_All_Metrics'] = df_all_metrics
            
            # Get power metrics
            df_power_metrics = pd.read_sql_query(POWER_METRICS_QUERY, client.db_connection)
            results['Compute_Power_Metrics'] = df_power_metrics
            
            # Get power metrics with specific units
            df_power_metrics_units = pd.read_sql_query(POWER_METRICS_QUERY_UNIT_IN_MW_W_KW, client.db_connection)
            results['Compute_Power_Metrics_Units'] = df_power_metrics_units
            
            # Get temperature metrics
            df_temp_metrics = pd.read_sql_query(TEMPERATURE_METRICS, client.db_connection)
            results['Compute_Temperature_Metrics'] = df_temp_metrics
            
            print(f"  - Compute: {len(df_all_metrics)} total metrics, {len(df_power_metrics)} power metrics")
            
        except Exception as e:
            print(f"❌ Error collecting compute metrics: {e}")
        
        return results
    
    def _collect_infra_metrics(self, client) -> Dict[str, pd.DataFrame]:
        """Collect infrastructure database metrics"""
        results = {}
        
        try:
            # Get all IRC infrastructure metrics
            df_all_metrics = pd.read_sql_query(ALL_INFRA_METRICS, client.db_connection)
            results['Infra_All_Metrics'] = df_all_metrics
            
            # Get IRC power-related metrics
            df_power_metrics = pd.read_sql_query(INFRA_POWER_METRICS, client.db_connection)
            results['Infra_Power_Metrics'] = df_power_metrics
            
            # Get IRC temperature metrics
            df_temp_metrics = pd.read_sql_query(TEMPERATURE_METRICS, client.db_connection)
            results['Infra_Temperature_Metrics'] = df_temp_metrics
            
            print(f"  - Infrastructure: {len(df_all_metrics)} total metrics, {len(df_power_metrics)} power metrics")
            
        except Exception as e:
            print(f"❌ Error collecting infrastructure metrics: {e}")
        
        return results
    
    def _create_excel_file(self, all_data: Dict[str, pd.DataFrame], output_path: str, 
                          specific_sheets: List[str] = None):
        """Create Excel file with all collected data"""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                sheet_count = 0
                
                for sheet_name, df in all_data.items():
                    if specific_sheets and sheet_name not in specific_sheets:
                        continue
                    
                    if not df.empty:
                        # Limit sheet name length
                        safe_sheet_name = sheet_name[:31]
                        df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                        sheet_count += 1
                        print(f"  - {safe_sheet_name}: {len(df)} rows")
                
                if sheet_count == 0:
                    raise ValueError("No data to write to Excel file")
                
                print(f"\n📊 Excel report created successfully: {output_path}")
                print(f"📋 Sheets: {sheet_count}")
                
        except Exception as e:
            print(f"❌ Error creating Excel file: {e}")
            raise
    
    def _create_rack_excel_file(self, rack_num: int, compute_results: Dict, pdu_results: Dict, output_path: str):
        """Create Excel file for rack analysis"""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Compute nodes sheet
                if compute_results:
                    compute_data = []
                    for hostname, (df, energy_dict) in compute_results.items():
                        if not df.empty:
                            df_copy = df.copy()
                            df_copy['hostname'] = hostname
                            compute_data.append(df_copy)
                    
                    if compute_data:
                        combined_compute = pd.concat(compute_data, ignore_index=True)
                        combined_compute.to_excel(writer, sheet_name='Compute_Nodes', index=False)
                
                # PDU nodes sheet
                if pdu_results:
                    pdu_data = []
                    for hostname, (df, energy_dict) in pdu_results.items():
                        if not df.empty:
                            df_copy = df.copy()
                            df_copy['hostname'] = hostname
                            pdu_data.append(df_copy)
                    
                    if pdu_data:
                        combined_pdu = pd.concat(pdu_data, ignore_index=True)
                        combined_pdu.to_excel(writer, sheet_name='PDU_Nodes', index=False)
                
                # Summary sheet
                summary_data = {
                    'Rack_Number': [rack_num],
                    'Analysis_Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                    'Compute_Nodes_Count': [len(compute_results) if compute_results else 0],
                    'PDU_Nodes_Count': [len(pdu_results) if pdu_results else 0]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
        except Exception as e:
            print(f"❌ Error creating rack Excel file: {e}")
            raise
