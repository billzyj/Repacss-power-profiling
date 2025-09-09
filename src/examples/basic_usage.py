#!/usr/bin/env python3
"""
Comprehensive example of the REPACSS Power Measurement Client
Demonstrates both single database and multi-database functionality
"""

import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import (
    connect_to_database,
    connect_to_all_databases,
    disconnect_all,
    get_client,
    get_all_clients,
    create_client_for_database
)
from core.config import config


def single_database_example():
    """Example using a single database (default: h100)"""
    
    print("=" * 60)
    print(f"SINGLE DATABASE EXAMPLE ({config.db_default_name})")
    print("=" * 60)
    
    # Create client for default database
    client = create_client_for_database(config.db_default_name)
    
    try:
        # Connect to database
        print("Connecting to REPACSS TimescaleDB...")
        client.connect()
        print("✓ Connected successfully!")
        
        # Example 1: Get available metrics
        print("\n=== Available Metrics ===")
        metrics = client.get_available_idrac_metrics()
        if metrics:
            print("Available metrics:")
            for metric in metrics[:10]:  # Show first 10
                print(f"  - {metric}")
            if len(metrics) > 10:
                print(f"  ... and {len(metrics) - 10} more")
        else:
            print("No metrics found")
        
        # Example 2: Get recent compute power metrics
        print("\n=== Recent Compute Power Metrics ===")
        power_metrics = client.get_computepower_metrics(limit=5)
        if power_metrics:
            for metric in power_metrics:
                print(f"  {metric['timestamp']} - Node {metric['nodeid']} "
                      f"(Source: {metric['source']}, FQDD: {metric['fqdd']}) - "
                      f"Power: {metric['value']:.1f}W")
        else:
            print("  No recent compute power metrics found")
        
        # Example 3: Get recent board temperature metrics
        print("\n=== Recent Board Temperature Metrics ===")
        temp_metrics = client.get_boardtemperature_metrics(limit=5)
        if temp_metrics:
            for metric in temp_metrics:
                print(f"  {metric['timestamp']} - Node {metric['nodeid']} "
                      f"(Source: {metric['source']}, FQDD: {metric['fqdd']}) - "
                      f"Temperature: {metric['value']:.1f}°C")
        else:
            print("  No recent board temperature metrics found")
        
        # Example 4: Get cluster summary
        print("\n=== Cluster Summary (last hour) ===")
        summary = client.get_idrac_cluster_summary()
        if summary and summary.get('total_nodes'):
            print(f"  Total Nodes: {summary['total_nodes']}")
            if summary.get('cluster_avg_power') is not None:
                print(f"  Average Power: {summary['cluster_avg_power']:.1f}W")
            if summary.get('cluster_max_power') is not None:
                print(f"  Max Power: {summary['cluster_max_power']:.1f}W")
            if summary.get('cluster_avg_temp') is not None:
                print(f"  Average Temperature: {summary['cluster_avg_temp']:.1f}°C")
            if summary.get('cluster_max_temp') is not None:
                print(f"  Max Temperature: {summary['cluster_max_temp']:.1f}°C")
            print(f"  Power Data Points: {summary.get('power_data_points', 0)}")
            print(f"  Temperature Data Points: {summary.get('temp_data_points', 0)}")
        else:
            print("  No cluster data available")
        
        # Example 5: Custom query for high power usage
        print("\n=== Custom Query: High Compute Power Usage ===")
        custom_query = f"""
        SELECT 
            timestamp,
            nodeid,
            value as power_watts
        FROM {client.schema}.computepower
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        AND value > 1000
        ORDER BY value DESC
        LIMIT 10
        """
        
        try:
            results = client.execute_query(custom_query)
            if results:
                print("  High power usage events (>1000W):")
                for i, row in enumerate(results[:5]):
                    print(f"    {i+1}. {row[0]} - Node {row[1]}: {row[2]:.1f}W")
                if len(results) > 5:
                    print(f"    ... and {len(results) - 5} more events")
            else:
                print("  No high power usage events found")
        except Exception as e:
            print(f"  Error executing custom query: {e}")
        
    except Exception as e:
        print(f"❌ Error in single database example: {e}")
    finally:
        client.disconnect()
        print("\n✓ Disconnected from database")


def multi_database_example():
    """Example using multiple databases"""
    
    print("\n" + "=" * 60)
    print("MULTI-DATABASE EXAMPLE")
    print("=" * 60)
    
    # Connect to all available databases
    clients = connect_to_all_databases()
    
    if not clients:
        print("❌ No databases connected successfully")
        return
    
    try:
        # Compare power metrics across databases
        print("\n=== Power Comparison Across Databases ===")
        for database_name, client in clients.items():
            try:
                summary = client.get_computepower_summary()
                if summary:
                    print(f"  {database_name.upper()}:")
                    print(f"    Average Power: {summary.get('avg_power', 0):.1f}W")
                    print(f"    Max Power: {summary.get('max_power', 0):.1f}W")
                    print(f"    Data Points: {summary.get('data_points', 0)}")
                else:
                    print(f"  {database_name.upper()}: No power data available")
            except Exception as e:
                print(f"  {database_name.upper()}: Error - {e}")
        
        # Compare temperature metrics across databases
        print("\n=== Temperature Comparison Across Databases ===")
        for database_name, client in clients.items():
            try:
                summary = client.get_boardtemperature_summary()
                if summary:
                    print(f"  {database_name.upper()}:")
                    print(f"    Average Temperature: {summary.get('avg_temp', 0):.1f}°C")
                    print(f"    Max Temperature: {summary.get('max_temp', 0):.1f}°C")
                    print(f"    Data Points: {summary.get('data_points', 0)}")
                else:
                    print(f"  {database_name.upper()}: No temperature data available")
            except Exception as e:
                print(f"  {database_name.upper()}: Error - {e}")
        
        # Get recent metrics from each database
        print("\n=== Recent Metrics from Each Database ===")
        for database_name, client in clients.items():
            try:
                print(f"\n  {database_name.upper()} - Recent Power Metrics:")
                power_metrics = client.get_computepower_metrics(limit=3)
                if power_metrics:
                    for metric in power_metrics:
                        print(f"    {metric['timestamp']} - Node {metric['nodeid']}: {metric['value']:.1f}W")
                else:
                    print("    No recent power metrics")
                
                print(f"  {database_name.upper()} - Recent Temperature Metrics:")
                temp_metrics = client.get_boardtemperature_metrics(limit=3)
                if temp_metrics:
                    for metric in temp_metrics:
                        print(f"    {metric['timestamp']} - Node {metric['nodeid']}: {metric['value']:.1f}°C")
                else:
                    print("    No recent temperature metrics")
                    
            except Exception as e:
                print(f"  {database_name.upper()}: Error - {e}")
    
    finally:
        # Disconnect from all databases
        disconnect_all()


def main():
    """Main function to run all examples"""
    print("🚀 REPACSS Power Measurement Client Examples")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print(f"🗄️  Available databases: {', '.join(config.databases)}")
    print()
    
    try:
        # Run single database example
        single_database_example()
        
        # Run multi-database example
        multi_database_example()
        
        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        print(f"\n📅 Finished at: {datetime.now()}")


if __name__ == "__main__":
    main() 