# Run public queries and output to Excel
import pandas as pd
from datetime import datetime
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.compute.public import *
from queries.infra.public import *
from database.database import (
    connect_to_database, 
    disconnect_all,
    get_client
)

def get_h100_zen4_power_metrics():
    """Get all power metrics from H100/ZEN4 database using SSH tunnel"""
    try:
        # Connect to H100 database with public schema
        client = connect_to_database('h100', 'public')
        if not client:
            print("❌ Failed to connect to H100 database")
            return {}
        
        # Get all power metrics
        df_power_metrics = pd.read_sql_query(POWER_METRICS_QUERY, client.db_connection)
        
        return {
            'Power_Metrics': df_power_metrics,
        }
    
    except Exception as e:
        print(f"Error getting H100/ZEN4 power metrics: {e}")
        return {}

def get_compute_public_metrics():
    """Get all public schema metrics from compute databases (h100, zen4)"""
    try:
        # Connect to h100 database with public schema since h100 and zen4 share the same public schema
        client = connect_to_database('h100', 'public')
        
        if not client:
            print("❌ Failed to connect to h100 database")
            return {}
        
        print("📊 Collecting compute public metrics (h100/zen4 shared schema)...")
        
        results = {}
        
        # Get all metrics
        df_all_metrics = pd.read_sql_query(ALL_METRICS, client.db_connection)
        
        # Get power metrics
        df_power_metrics = pd.read_sql_query(POWER_METRICS_QUERY, client.db_connection)
        
        # Get power metrics with specific units
        df_power_metrics_units = pd.read_sql_query(POWER_METRICS_QUERY_UNIT_IN_MW_W_KW, client.db_connection)
        
        # Get temperature metrics
        df_temp_metrics = pd.read_sql_query(TEMPERATURE_METRICS, client.db_connection)
        
        results['Compute_All_Metrics'] = df_all_metrics
        results['Compute_Power_Metrics'] = df_power_metrics
        results['Compute_Power_Metrics_Units'] = df_power_metrics_units
        results['Compute_Temperature_Metrics'] = df_temp_metrics
        
        print(f"  - Compute: {len(df_all_metrics)} total metrics, {len(df_power_metrics)} power metrics")
        
        return results
    
    except Exception as e:
        print(f"Error getting compute public metrics: {e}")
        return {}

def get_infra_public_metrics():
    """Get all public schema metrics from INFRA database"""
    try:
        # Connect to INFRA database with public schema
        client = connect_to_database('infra', 'public')
        if not client:
            print("❌ Failed to connect to INFRA database")
            return {}
        
        print("📊 Collecting INFRA public metrics...")
        
        # Get all IRC infrastructure metrics
        df_all_metrics = pd.read_sql_query(ALL_INFRA_METRICS, client.db_connection)
        
        # Get IRC power-related metrics
        df_power_metrics = pd.read_sql_query(INFRA_POWER_METRICS, client.db_connection)
        
        # Get IRC temperature metrics
        df_temp_metrics = pd.read_sql_query(TEMPERATURE_METRICS, client.db_connection)
        
        # Get IRC compressor metrics
        df_compressor_metrics = pd.read_sql_query(COMPRESSOR_METRICS, client.db_connection)
        
        # Get IRC air system metrics
        df_air_metrics = pd.read_sql_query(AIR_SYSTEM_METRICS, client.db_connection)
        
        # Get IRC run hours metrics
        df_run_hours_metrics = pd.read_sql_query(RUN_HOURS_METRICS, client.db_connection)
        
        # Get IRC humidity metrics
        df_humidity_metrics = pd.read_sql_query(HUMIDITY_METRICS, client.db_connection)
        
        # Get IRC pressure metrics
        df_pressure_metrics = pd.read_sql_query(PRESSURE_METRICS, client.db_connection)
        
        # Get nodes with metrics
        df_nodes_with_metrics = pd.read_sql_query(NODES_WITH_METRICS, client.db_connection)
        
        print(f"  - INFRA: {len(df_all_metrics)} total metrics, {len(df_power_metrics)} power metrics")
        
        return {
            'INFRA_All_Metrics': df_all_metrics,
            'INFRA_Power_Metrics': df_power_metrics,
            'INFRA_Temperature_Metrics': df_temp_metrics,
            'INFRA_Compressor_Metrics': df_compressor_metrics,
            'INFRA_Air_System_Metrics': df_air_metrics,
            'INFRA_Run_Hours_Metrics': df_run_hours_metrics,
            'INFRA_Humidity_Metrics': df_humidity_metrics,
            'INFRA_Pressure_Metrics': df_pressure_metrics,
            'INFRA_Nodes_With_Metrics': df_nodes_with_metrics
        }
    
    except Exception as e:
        print(f"Error getting INFRA public metrics: {e}")
        return {}

def create_excel_report(compute_data, infra_data, output_filename=None):
    """Create Excel report with separate sheets for each dataset"""
    
    # Create output directory if it doesn't exist
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    if output_filename is None:
        # Use simple naming without timestamp
        output_filename = "public_metrics_report.xlsx"
    
    # Full path to output file
    output_path = os.path.join(output_dir, output_filename)
    
    # Filter out empty DataFrames
    compute_data = {k: v for k, v in compute_data.items() if not v.empty}
    infra_data = {k: v for k, v in infra_data.items() if not v.empty}
    
    # Check if we have any data to write
    if not compute_data and not infra_data:
        print("❌ No data available to write to Excel report")
        return None
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Write compute data (h100, zen4)
            print("Writing compute public metrics...")
            for sheet_name, df in compute_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  - {sheet_name}: {len(df)} rows")
            
            # Write INFRA data
            print("Writing INFRA public metrics...")
            for sheet_name, df in infra_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  - {sheet_name}: {len(df)} rows")
        
        print(f"\nExcel report created successfully: {output_path}")
        return output_path
    
    except Exception as e:
        print(f"Error creating Excel report: {e}")
        return None

def main():
    """Main function to run all public schema queries and create Excel report"""
    print("Starting public schema metrics data collection...")
    print("=" * 50)
    
    try:
        # Get compute public metrics (h100, zen4)
        print("Collecting compute public metrics...")
        compute_data = get_compute_public_metrics()
        
        # Get INFRA public metrics
        print("Collecting INFRA public metrics...")
        infra_data = get_infra_public_metrics()
        
        # Create Excel report for public queries
        print("\nCreating Excel report for public queries...")
        output_file = create_excel_report(compute_data, infra_data)
        
        if output_file:
            print(f"\nPublic queries report summary:")
            print(f"  - Compute sheets: {len(compute_data)}")
            print(f"  - INFRA sheets: {len(infra_data)}")
            print(f"  - Output file: {output_file}")
            
            # Print summary statistics
            print(f"\nData summary:")
            for sheet_name, df in compute_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            
            for sheet_name, df in infra_data.items():
                if not df.empty:
                    print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
        else:
            print("Failed to create Excel report")
    
    finally:
        # Disconnect from all databases
        disconnect_all()

if __name__ == "__main__":
    main()




