#!/usr/bin/env python3
"""
Node Level Power Queries Runner
Runs power consumption queries on both zen4 and h100 databases
and saves results to Excel files with separate sheets for each metric.
"""

import pandas as pd
import sys
import os
import argparse
from datetime import datetime
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Get project root directory (parent of src)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from queries.compute.idrac import (
    get_power_metrics_with_names,
    get_power_metrics_with_names_by_time_range
)
from core.database import (
    connect_to_database, 
    connect_to_specific_databases,
    disconnect_all,
    get_client
)

def get_power_queries_for_node(node_id: int, hours_back: int = 360) -> Dict[str, str]:
    """
    Get all power queries for a specific node over the past 15 days
    
    Args:
        node_id: Node ID to query
        hours_back: Number of hours to look back (default: 360 = 15 days)
    """
    queries = {}
    
    # System Power Consumption (Total)
    queries["System Power"] = f"""
    SELECT 
        p.timestamp,
        n.hostname,
        p.value as value,
        'System Power' as source,
        'Total System' as fqdd
    FROM idrac.systempowerconsumption p
    LEFT JOIN public.nodes n ON p.nodeid = n.nodeid
    WHERE p.nodeid = {node_id} 
    AND p.timestamp >= NOW() - INTERVAL '{hours_back} hours'
    ORDER BY p.timestamp DESC;
    """
    
    # GPU Power Consumption (convert from mW to W)
    queries["GPU Power"] = f"""
    SELECT 
        p.timestamp,
        n.hostname,
        p.value / 1000.0 as value,  -- Convert mW to W
        'GPU Power' as source,
        'GPU' as fqdd
    FROM idrac.powerconsumption p
    LEFT JOIN public.nodes n ON p.nodeid = n.nodeid
    WHERE p.nodeid = {node_id} 
    AND p.timestamp >= NOW() - INTERVAL '{hours_back} hours'
    ORDER BY p.timestamp DESC;
    """
    
    # Component power tables (every 5 mins)
    component_tables = [
        ('totalcpupower', 'CPU Power'),
        ('totalfanpower', 'Fan Power'),
        ('totalfpgapower', 'FPGA Power'),
        ('totalmemorypower', 'Memory Power'),
        ('totalpciepower', 'PCIe Power'),
        ('totalstoragepower', 'Storage Power')
    ]
    
    for table, display_name in component_tables:
        queries[display_name] = f"""
        SELECT 
            p.timestamp,
            n.hostname,
            p.value as value,
            '{display_name}' as source,
            '{display_name}' as fqdd
        FROM idrac.{table} p
        LEFT JOIN public.nodes n ON p.nodeid = n.nodeid
        WHERE p.nodeid = {node_id} 
        AND p.timestamp >= NOW() - INTERVAL '{hours_back} hours'
        ORDER BY p.timestamp DESC;
        """
    
    return queries

def run_queries_on_database(client, queries: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    Run all power queries on a database and return results as DataFrames
    
    Args:
        client: Database client instance
        queries: Dictionary of query names and SQL queries
    
    Returns:
        Dictionary mapping query names to DataFrames
    """
    results = {}
    
    for i, (query_name, query) in enumerate(queries.items(), 1):
        try:
            print(f"Running {query_name} ({i}/{len(queries)})...")
            print(f"  Query: {query[:100]}...")  # Show first 100 chars of query
            
            # Add timeout and chunking for large queries
            df = pd.read_sql_query(query, client.db_connection, chunksize=1000)
            
            # If chunksize returns an iterator, collect all chunks
            if hasattr(df, '__iter__'):
                chunks = []
                for chunk in df:
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            
            results[query_name] = df
            print(f"✓ {query_name}: {len(df)} records retrieved")
            
        except Exception as e:
            print(f"✗ Error running {query_name}: {str(e)}")
            # Create empty DataFrame with expected columns
            results[query_name] = pd.DataFrame(columns=['timestamp', 'hostname', 'value', 'source', 'fqdd'])
    
    return results

def create_power_graphs_for_node(results: Dict[str, pd.DataFrame], output_dir: str, database_name: str, node_id: int):
    """
    Create matplotlib graphs for power consumption data for a specific node
    
    Args:
        results: Dictionary mapping query names to DataFrames
        output_dir: Output directory for graphs
        database_name: Name of the database (for title and filename)
        node_id: Node ID for the graph title and filename
    """
    try:
        # Set up the plot style
        plt.style.use('default')
        rcParams['figure.figsize'] = (16, 10)
        rcParams['font.size'] = 22  # Increased base font size
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(16, 10))
        
        # Define colors for each power type (8 different colors)
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', 
                 '#9467bd', '#8c564b', '#e377c2', '#17becf']
        
        # Plot each power metric
        for i, (power_name, df) in enumerate(results.items()):
            if df.empty:
                print(f"⚠️  No data for {power_name}, skipping...")
                continue
                
            # Convert timestamp to datetime if it's not already
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Group by timestamp and calculate mean power for each timestamp
                # This handles multiple readings at the same time
                grouped_data = df.groupby('timestamp')['value'].mean().reset_index()
                
                # Plot the line
                ax.plot(grouped_data['timestamp'], grouped_data['value'], 
                       label=power_name, color=colors[i % len(colors)], 
                       linewidth=2, alpha=0.8)
        
        # Customize the plot with increased font sizes
        ax.set_xlabel('Time', fontsize=22, fontweight='bold')  # Increased from 14
        ax.set_ylabel('Power (Watts)', fontsize=22, fontweight='bold')  # Increased from 14
        ax.set_title(f'Power Consumption Over Time - {database_name.upper()} Database - Node {node_id}', 
                    fontsize=26, fontweight='bold', pad=20)  # Increased from 16
        
        # Format x-axis (time) - show hours for week-long data
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=12))  # Show every 12 hours
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=14)  # Added fontsize
        
        # Format y-axis tick labels
        plt.setp(ax.yaxis.get_majorticklabels(), fontsize=20)  # Added fontsize
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add legend inside the plot in upper right corner
        ax.legend(loc='upper right', fontsize=22)  # Increased fontsize and moved inside
        
        # Adjust layout to prevent label cutoff
        plt.tight_layout()
        
        # Save the plot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(output_dir, f"power_consumption_{database_name}_node{node_id}_{timestamp}.png")
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        
        print(f"✓ Power consumption graph for Node {node_id} saved to {filename}")
        
        # Also save as PDF for vector graphics
        pdf_filename = os.path.join(output_dir, f"power_consumption_{database_name}_node{node_id}_{timestamp}.pdf")
        plt.savefig(pdf_filename, bbox_inches='tight')
        print(f"✓ Power consumption graph for Node {node_id} saved to {pdf_filename}")
        
        # Show the plot
        plt.show()
        
    except Exception as e:
        print(f"✗ Error creating power graphs: {str(e)}")
        import traceback
        traceback.print_exc()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Generate power consumption graphs for H100 and/or ZEN4 nodes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_node_level_queries.py --database h100          # Process only H100 nodes
  python run_node_level_queries.py --database zen4          # Process only ZEN4 nodes  
  python run_node_level_queries.py --database both          # Process both databases
  python run_node_level_queries.py --database h100 --hours 168  # H100 with 7 days
  python run_node_level_queries.py --database h100 --node 4     # H100 node 4 only
  python run_node_level_queries.py --database zen4 --node 4     # ZEN4 node 4 only
        """
    )
    
    parser.add_argument(
        '--database', 
        choices=['h100', 'zen4', 'both'], 
        default='both',
        help='Database to process: h100, zen4, or both (default: both)'
    )
    
    parser.add_argument(
        '--hours', 
        type=int, 
        default=360,
        help='Number of hours to look back (default: 360 = 15 days)'
    )
    
    parser.add_argument(
        '--node',
        type=int,
        choices=range(1, 9),  # 1-8 for H100, 1-4 for ZEN4
        help='Specific node to process (1-8 for H100, 1-4 for ZEN4). If not specified, processes all nodes.'
    )
    
    return parser.parse_args()

def main():
    """Main function to run power queries for selected databases"""
    args = parse_arguments()
    
    print("Starting Node Level Power Queries...")
    print("=" * 50)
    
    # Configuration based on arguments
    HOURS_BACK = args.hours
    
    # Determine node ranges based on arguments
    if args.node is not None:
        # Single node mode
        H100_NODE_RANGE = [args.node] if args.node <= 8 else []
        ZEN4_NODE_RANGE = [args.node] if args.node <= 4 else []
        print(f"⚠️  Single node mode: Node {args.node}")
    else:
        # All nodes mode
        H100_NODE_RANGE = range(1, 9)  # Nodes 1-8 for H100
        ZEN4_NODE_RANGE = range(1, 5)  # Nodes 1-4 for ZEN4
    
    # Determine which databases to process
    process_h100 = args.database in ['h100', 'both']
    process_zen4 = args.database in ['zen4', 'both']
    
    print(f"Configuration:")
    print(f"  - Database(s): {args.database.upper()}")
    if process_h100:
        print(f"  - H100 Nodes: {list(H100_NODE_RANGE)}")
    if process_zen4:
        print(f"  - ZEN4 Nodes: {list(ZEN4_NODE_RANGE)}")
    print(f"  - Time range: Last {HOURS_BACK} hours ({HOURS_BACK//24} days)")
    print()
    
    try:
        # Connect to databases based on selection
        h100_client = None
        zen4_client = None
        
        if process_h100:
            print("Connecting to H100 database...")
            h100_client = connect_to_database('h100', 'idrac')
            if not h100_client:
                print("❌ Failed to connect to H100 database")
                return
        
        if process_zen4:
            print("Connecting to ZEN4 database...")
            zen4_client = connect_to_database('zen4', 'idrac')
            if not zen4_client:
                print("❌ Failed to connect to ZEN4 database")
                return
        
        print("✓ Database clients initialized")
        
        # Create output directory in project root if it doesn't exist
        output_dir = os.path.join(project_root, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        # Process H100 nodes
        h100_results = {}
        if process_h100:
            print("\n" + "=" * 50)
            print("PROCESSING H100 DATABASE")
            print("=" * 50)
            
            for node_id in H100_NODE_RANGE:
                print(f"\n" + "=" * 30)
                print(f"Processing H100 Node {node_id}...")
                print("=" * 30)
                
                # Get queries for this specific node
                queries = get_power_queries_for_node(node_id, hours_back=HOURS_BACK)
                print(f"Found {len(queries)} power queries to run for H100 Node {node_id}")
                
                # Run queries for this node
                node_results = run_queries_on_database(h100_client, queries)
                h100_results[node_id] = node_results
                
                # Create graph for this node
                print(f"\nCreating power consumption graph for H100 Node {node_id}...")
                create_power_graphs_for_node(node_results, output_dir, "h100", node_id)
                
                print(f"✓ Completed processing for H100 Node {node_id}")
        
        # Process ZEN4 nodes
        zen4_results = {}
        if process_zen4:
            print("\n" + "=" * 50)
            print("PROCESSING ZEN4 DATABASE")
            print("=" * 50)
            
            for node_id in ZEN4_NODE_RANGE:
                print(f"\n" + "=" * 30)
                print(f"Processing ZEN4 Node {node_id}...")
                print("=" * 30)
                
                # Get queries for this specific node
                queries = get_power_queries_for_node(node_id, hours_back=HOURS_BACK)
                print(f"Found {len(queries)} power queries to run for ZEN4 Node {node_id}")
                
                # Run queries for this node
                node_results = run_queries_on_database(zen4_client, queries)
                zen4_results[node_id] = node_results
                
                # Create graph for this node
                print(f"\nCreating power consumption graph for ZEN4 Node {node_id}...")
                create_power_graphs_for_node(node_results, output_dir, "zen4", node_id)
                
                print(f"✓ Completed processing for ZEN4 Node {node_id}")
        
        # Print summary
        print("\n" + "=" * 50)
        print("SUMMARY")
        print("=" * 50)
        
        if process_h100:
            print("\nH100 Database Results:")
            for node_id in H100_NODE_RANGE:
                print(f"\n  H100 Node {node_id}:")
                if node_id in h100_results:
                    for query_name, df in h100_results[node_id].items():
                        print(f"    {query_name}: {len(df)} records")
                else:
                    print("    No data available")
        
        if process_zen4:
            print("\nZEN4 Database Results:")
            for node_id in ZEN4_NODE_RANGE:
                print(f"\n  ZEN4 Node {node_id}:")
                if node_id in zen4_results:
                    for query_name, df in zen4_results[node_id].items():
                        print(f"    {query_name}: {len(df)} records")
                else:
                    print("    No data available")
        
        print(f"\nGraphs created:")
        if process_h100:
            print(f"  - {len(H100_NODE_RANGE)} H100 node power consumption graphs (PNG and PDF)")
            print(f"  - Files: power_consumption_h100_node1-8_*.png/pdf")
        if process_zen4:
            print(f"  - {len(ZEN4_NODE_RANGE)} ZEN4 node power consumption graphs (PNG and PDF)")
            print(f"  - Files: power_consumption_zen4_node1-4_*.png/pdf")
        
        print("\n✓ Node level power queries completed successfully!")
    
    finally:
        # Disconnect from all databases
        disconnect_all()

if __name__ == "__main__":
    main()
