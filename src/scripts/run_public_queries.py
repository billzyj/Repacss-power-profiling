# Run public queries and output to Excel
import pandas as pd
from datetime import datetime
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.compute.public import *
from queries.compute.idrac import get_power_metrics_with_joins
from queries.infra.public import *
from core.database import (
    connect_to_database, 
    connect_to_specific_databases,
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

def get_metric_data_with_joins():
    """Get data for each power metric with joins to get hostname, source, and fqdd names"""
    try:
        # Connect to H100 database with public schema
        client = connect_to_database('h100', 'public')
        if not client:
            print("❌ Failed to connect to H100 database")
            return {}
        
        # First get the list of power metrics
        df_power_metrics = pd.read_sql_query(POWER_METRICS_QUERY, client.db_connection)
        
        if df_power_metrics.empty:
            print("No power metrics found")
            return {}
        
        # Get metric IDs (convert to lowercase for idrac schema)
        metric_ids = df_power_metrics['metric_id'].str.lower().tolist()
        
        print(f"Found {len(metric_ids)} power metrics to process")
        
        # Dictionary to store results
        results = {}
        
        # Process each metric
        for metric_id in metric_ids:
            try:
                print(f"Processing metric: {metric_id}")
                
                # Get the query for this metric
                query = get_power_metrics_with_joins(metric_id, limit=1000)
                
                # Execute the query
                df_metric_data = pd.read_sql_query(query, client.db_connection)
                
                if not df_metric_data.empty:
                    # Convert timestamp to timezone-unaware for Excel compatibility
                    if 'timestamp' in df_metric_data.columns:
                        df_metric_data['timestamp'] = df_metric_data['timestamp'].dt.tz_localize(None)
                    
                    # Use the original metric ID (with proper case) as sheet name
                    original_metric_id = df_power_metrics[df_power_metrics['metric_id'].str.lower() == metric_id]['metric_id'].iloc[0]
                    results[original_metric_id] = df_metric_data
                    print(f"  - {original_metric_id}: {len(df_metric_data)} rows")
                else:
                    print(f"  - {metric_id}: No data found")
                    
            except Exception as e:
                print(f"  - Error processing {metric_id}: {e}")
                continue
        
        return results
    
    except Exception as e:
        print(f"Error getting metric data with joins: {e}")
        return {}

def get_infra_power_metrics():
    """Get all power metrics from INFRA database using SSH tunnel"""
    try:
        # Connect to INFRA database with public schema
        client = connect_to_database('infra', 'public')
        if not client:
            print("❌ Failed to connect to INFRA database")
            return {}
        
        # Get all IRC infrastructure metrics
        df_all_metrics = pd.read_sql_query(ALL_INFRA_METRICS, client.db_connection)
        
        # Get IRC power-related metrics
        df_power_metrics = pd.read_sql_query(INFRA_POWER_METRICS, client.db_connection)
        
        # Get IRC metrics by units
        df_metrics_by_units = pd.read_sql_query(INFRA_METRICS_BY_UNITS, client.db_connection)
        
        # Get IRC metrics by data type
        df_metrics_by_type = pd.read_sql_query(INFRA_METRICS_BY_TYPE, client.db_connection)
        
        # Get all nodes
        df_all_nodes = pd.read_sql_query(ALL_NODES, client.db_connection)
        
        # Get PDU nodes
        df_pdu_nodes = pd.read_sql_query(PDU_NODES, client.db_connection)
        
        # Get rack cooling nodes
        df_rack_cooling_nodes = pd.read_sql_query(RACK_COOLING_NODES, client.db_connection)
        
        # Get node count by type
        df_node_count = pd.read_sql_query(NODE_COUNT_BY_TYPE, client.db_connection)
        
        # Get nodes with metrics
        df_nodes_with_metrics = pd.read_sql_query(NODES_WITH_METRICS, client.db_connection)
        
        return {
            'All_IRC_Metrics': df_all_metrics,
            'IRC_Power_Metrics': df_power_metrics,
            'IRC_Metrics_By_Units': df_metrics_by_units,
            'IRC_Metrics_By_Type': df_metrics_by_type,
            'All_Nodes': df_all_nodes,
            'PDU_Nodes': df_pdu_nodes,
            'Rack_Cooling_Nodes': df_rack_cooling_nodes,
            'Node_Count_By_Type': df_node_count,
            'Nodes_With_Metrics': df_nodes_with_metrics
        }
    
    except Exception as e:
        print(f"Error getting INFRA power metrics: {e}")
        return {}

def create_excel_report(h100_zen4_data, infra_data, output_filename=None):
    """Create Excel report with separate sheets for each dataset"""
    
    # Create output directory if it doesn't exist
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    if output_filename is None:
        # Use simple naming without timestamp
        output_filename = "power_metrics_report.xlsx"
    
    # Full path to output file
    output_path = os.path.join(output_dir, output_filename)
    
    # Filter out empty DataFrames
    h100_zen4_data = {k: v for k, v in h100_zen4_data.items() if not v.empty}
    infra_data = {k: v for k, v in infra_data.items() if not v.empty}
    
    # Check if we have any data to write
    if not h100_zen4_data and not infra_data:
        print("❌ No data available to write to Excel report")
        return None
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Write H100/ZEN4 data
            print("Writing H100/ZEN4 power metrics...")
            for sheet_name, df in h100_zen4_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  - {sheet_name}: {len(df)} rows")
            
            # Write INFRA data
            print("Writing INFRA power metrics...")
            for sheet_name, df in infra_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  - {sheet_name}: {len(df)} rows")
        
        print(f"\nExcel report created successfully: {output_path}")
        return output_path
    
    except Exception as e:
        print(f"Error creating Excel report: {e}")
        return None

def main():
    """Main function to run all queries and create Excel report"""
    print("Starting power metrics data collection...")
    print("=" * 50)
    
    try:
        # Get H100/ZEN4 power metrics
        print("Collecting H100/ZEN4 power metrics...")
        h100_zen4_data = get_h100_zen4_power_metrics()
        
        # Get INFRA power metrics
        print("Collecting INFRA power metrics...")
        infra_data = get_infra_power_metrics()
        
        # Create Excel report for public queries
        print("\nCreating Excel report for public queries...")
        output_file = create_excel_report(h100_zen4_data, infra_data)
        
        if output_file:
            print(f"\nPublic queries report summary:")
            print(f"  - H100/ZEN4 sheets: {len(h100_zen4_data)}")
            print(f"  - INFRA sheets: {len(infra_data)}")
            print(f"  - Output file: {output_file}")
        
        # Get metric data with joins for each power metric
        print("\nCollecting metric data with joins...")
        metric_data = get_metric_data_with_joins()
        
        if metric_data:
            # Create Excel report for metric data
            print("\nCreating Excel report for metric data...")
            metric_output_file = create_excel_report(metric_data, {}, "metric_data_with_joins.xlsx")
            
            if metric_output_file:
                print(f"\nMetric data report summary:")
                print(f"  - Metric sheets: {len(metric_data)}")
                print(f"  - Output file: {metric_output_file}")
                
                # Print summary statistics
                print(f"\nMetric data summary:")
                for sheet_name, df in metric_data.items():
                    if not df.empty:
                        print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
            else:
                print("Failed to create metric data Excel report")
        else:
            print("No metric data found")
    
    finally:
        # Disconnect from all databases
        disconnect_all()

if __name__ == "__main__":
    main()




