#!/usr/bin/env python3
"""
Run Rack Related Queries (PDU and IRC) from INFRA Database
Handles infra.idrac schema queries for PDU and IRC infrastructure monitoring
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.infra.irc_pdu import *
from core.database import (
    connect_to_database,
    disconnect_all
)


class RackRelatedQueryRunner:
    """Manages rack-related queries for PDU and IRC infrastructure"""
    
    def __init__(self):
        self.client = None
        
    def connect_to_infra(self):
        """Connect to INFRA database"""
        print("🔌 Connecting to INFRA database...")
        self.client = connect_to_database('infra', 'idrac')
        
        if not self.client:
            print("❌ Failed to connect to INFRA database")
            return False
        
        print("✓ Connected to INFRA database")
        return True
    
    def disconnect(self):
        """Disconnect from database"""
        if self.client:
            disconnect_all()
    
    def get_pdu_metrics(self, limit: int = 1000):
        """Get PDU power metrics with hostname and units"""
        try:
            print("📊 Collecting PDU power metrics...")
            
            # Get PDU power metrics
            df_pdu_power = pd.read_sql_query(
                PDU_POWER_WITH_HOSTNAME_AND_UNITS, 
                self.client.db_connection,
                params=[limit]
            )
            
            # Get PDU compressor power metrics
            df_pdu_compressor = pd.read_sql_query(
                PDU_COMPRESSORPOWER_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            
            return {
                'PDU_Power': df_pdu_power,
                'PDU_Compressor_Power': df_pdu_compressor
            }
            
        except Exception as e:
            print(f"Error getting PDU metrics: {e}")
            return {}
    
    def get_irc_metrics(self, limit: int = 1000):
        """Get IRC infrastructure metrics with hostname and units"""
        try:
            print("📊 Collecting IRC infrastructure metrics...")
            
            results = {}
            
            # Temperature metrics
            df_room_temp = pd.read_sql_query(
                IRC_ROOMTEMPERATURE_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Room_Temperature'] = df_room_temp
            
            # Pressure metrics
            df_suction_pressure = pd.read_sql_query(
                IRC_SUCTIONPRESSURE_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Suction_Pressure'] = df_suction_pressure
            
            df_discharge_pressure = pd.read_sql_query(
                IRC_DISCHARGEPRESSURE_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Discharge_Pressure'] = df_discharge_pressure
            
            # Fan and motor control metrics
            df_modulating_valve = pd.read_sql_query(
                IRC_MODULATINGVALVEPOSITION_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Modulating_Valve_Position'] = df_modulating_valve
            
            df_dry_cooler_fan = pd.read_sql_query(
                IRC_DRYCOOLERFANSPEED_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Dry_Cooler_Fan_Speed'] = df_dry_cooler_fan
            
            df_evaporator_fan = pd.read_sql_query(
                IRC_EVAPORATORFANSPEED_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Evaporator_Fan_Speed'] = df_evaporator_fan
            
            df_condenser_fan = pd.read_sql_query(
                IRC_CONDENSERFANSPEED_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Condenser_Fan_Speed'] = df_condenser_fan
            
            df_eev_position = pd.read_sql_query(
                IRC_EEVPOSITION_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_EEV_Position'] = df_eev_position
            
            # Power supply metrics
            df_fan_power_supply1 = pd.read_sql_query(
                IRC_FANPOWERSUPPLY1_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Fan_Power_Supply_1'] = df_fan_power_supply1
            
            df_fan_power_supply2 = pd.read_sql_query(
                IRC_FANPOWERSUPPLY2_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Fan_Power_Supply_2'] = df_fan_power_supply2
            
            # Run hours metrics
            df_fan6_run_hours = pd.read_sql_query(
                IRC_FAN6RUNHOURS_WITH_HOSTNAME_AND_UNITS,
                self.client.db_connection,
                params=[limit]
            )
            results['IRC_Fan6_Run_Hours'] = df_fan6_run_hours
            
            return results
            
        except Exception as e:
            print(f"Error getting IRC metrics: {e}")
            return {}
    
    def get_infrastructure_analysis(self):
        """Get infrastructure efficiency analysis and alerts"""
        try:
            print("📊 Collecting infrastructure analysis...")
            
            results = {}
            
            # Infrastructure efficiency analysis
            df_efficiency = pd.read_sql_query(
                INFRA_EFFICIENCY_ANALYSIS,
                self.client.db_connection
            )
            results['Infrastructure_Efficiency_Analysis'] = df_efficiency
            
            # High power usage alerts (threshold: 1000W)
            df_high_power = pd.read_sql_query(
                INFRA_HIGH_POWER_ALERTS,
                self.client.db_connection,
                params=[1000, 100]
            )
            results['High_Power_Alerts'] = df_high_power
            
            # High temperature alerts (threshold: 30°C)
            df_high_temp = pd.read_sql_query(
                INFRA_HIGH_TEMPERATURE_ALERTS,
                self.client.db_connection,
                params=[30, 100]
            )
            results['High_Temperature_Alerts'] = df_high_temp
            
            # Infrastructure cluster summary
            df_cluster_summary = pd.read_sql_query(
                INFRA_CLUSTER_SUMMARY,
                self.client.db_connection
            )
            results['Infrastructure_Cluster_Summary'] = df_cluster_summary
            
            return results
            
        except Exception as e:
            print(f"Error getting infrastructure analysis: {e}")
            return {}
    
    def get_time_range_analysis(self, hours: int = 24):
        """Get time range analysis for infrastructure metrics"""
        try:
            print(f"📊 Collecting {hours}-hour time range analysis...")
            
            results = {}
            
            # Get time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # PDU power summary
            pdu_summary_query = get_pdu_summary_with_hostname_and_units('power', hours)
            df_pdu_summary = pd.read_sql_query(pdu_summary_query, self.client.db_connection)
            results[f'PDU_Power_Summary_{hours}h'] = df_pdu_summary
            
            # IRC room temperature summary
            irc_temp_summary_query = get_irc_summary_with_hostname_and_units('roomtemperature', hours)
            df_irc_temp_summary = pd.read_sql_query(irc_temp_summary_query, self.client.db_connection)
            results[f'IRC_Temperature_Summary_{hours}h'] = df_irc_temp_summary
            
            return results
            
        except Exception as e:
            print(f"Error getting time range analysis: {e}")
            return {}
    
    def create_excel_report(self, pdu_data, irc_data, analysis_data, time_range_data, output_filename=None):
        """Create Excel report with separate sheets for each dataset"""
        
        # Create output directory if it doesn't exist
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"rack_related_queries_{timestamp}.xlsx"
        
        # Full path to output file
        output_path = os.path.join(output_dir, output_filename)
        
        # Combine all data
        all_data = {**pdu_data, **irc_data, **analysis_data, **time_range_data}
        
        # Filter out empty DataFrames
        all_data = {k: v for k, v in all_data.items() if not v.empty}
        
        # Check if we have any data to write
        if not all_data:
            print("❌ No data available to write to Excel report")
            return None
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                
                # Write PDU data
                print("Writing PDU metrics...")
                for sheet_name, df in pdu_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write IRC data
                print("Writing IRC metrics...")
                for sheet_name, df in irc_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write analysis data
                print("Writing infrastructure analysis...")
                for sheet_name, df in analysis_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
                
                # Write time range data
                print("Writing time range analysis...")
                for sheet_name, df in time_range_data.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  - {sheet_name}: {len(df)} rows")
            
            print(f"\nExcel report created successfully: {output_path}")
            return output_path
        
        except Exception as e:
            print(f"Error creating Excel report: {e}")
            return None


def main():
    """Main function to run all rack-related queries"""
    print("🚀 Rack Related Queries Runner (PDU & IRC)")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print()
    
    runner = RackRelatedQueryRunner()
    
    try:
        # Connect to INFRA database
        if not runner.connect_to_infra():
            return
        
        # Get PDU metrics
        pdu_data = runner.get_pdu_metrics(limit=1000)
        
        # Get IRC metrics
        irc_data = runner.get_irc_metrics(limit=1000)
        
        # Get infrastructure analysis
        analysis_data = runner.get_infrastructure_analysis()
        
        # Get time range analysis
        time_range_data = runner.get_time_range_analysis(hours=24)
        
        # Create Excel report
        print("\nCreating Excel report...")
        output_file = runner.create_excel_report(
            pdu_data, irc_data, analysis_data, time_range_data
        )
        
        if output_file:
            print(f"\nRack related queries report summary:")
            print(f"  - PDU sheets: {len([k for k, v in pdu_data.items() if not v.empty])}")
            print(f"  - IRC sheets: {len([k for k, v in irc_data.items() if not v.empty])}")
            print(f"  - Analysis sheets: {len([k for k, v in analysis_data.items() if not v.empty])}")
            print(f"  - Time range sheets: {len([k for k, v in time_range_data.items() if not v.empty])}")
            print(f"  - Output file: {output_file}")
            
            # Print summary statistics
            print(f"\nData summary:")
            for sheet_name, df in pdu_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            
            for sheet_name, df in irc_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            
            for sheet_name, df in analysis_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            
            for sheet_name, df in time_range_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
        else:
            print("Failed to create Excel report")
        
        print("✅ All rack-related queries completed successfully!")
        
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
