#!/usr/bin/env python3
"""
Run H100 Power Queries with Multiple Database Connections
Connects to h100, zen4, and infra databases simultaneously
"""

import sys
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import (
    connect_to_all_databases,
    connect_to_specific_databases,
    disconnect_all,
    get_client,
    get_all_clients
)
from core.config import config
from queries.compute.public import (
    POWER_METRICS_QUERY,
    ALL_POWER_METRICS,
    POWER_METRICS_BY_UNITS,
    HIGH_ACCURACY_POWER_METRICS,
    POWER_METRICS_BY_TYPE,
    POWER_FQDD_INFO,
    get_recent_power_data,
    get_power_summary
)
from queries.compute.idrac import (
    NODE_POWER_COMPARISON,
    POWER_EFFICIENCY_ANALYSIS as POWER_EFFICIENCY_METRICS
)


class MultiDatabaseQueryRunner:
    """Manages multiple database connections and runs queries"""
    
    def __init__(self):
        self.clients: Dict[str, any] = {}
        
    def connect_all_databases(self):
        """Connect to all available databases"""
        print("🔌 Connecting to databases...")
        
        self.clients = connect_to_all_databases()
        
        print(f"✓ Connected to {len(self.clients)} databases\n")
    
    def disconnect_all(self):
        """Disconnect from all databases"""
        disconnect_all()
    
    def run_query_on_all_databases(self, query_name: str, query: str, description: str = ""):
        """Run a query on all connected databases"""
        print(f"📊 {query_name}")
        if description:
            print(f"   {description}")
        print("=" * 60)
        
        for database_name, client in self.clients.items():
            try:
                print(f"\n🔍 {database_name.upper()} DATABASE:")
                print("-" * 40)
                
                with client.db_connection.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    if results:
                        # Get column names
                        column_names = [desc[0] for desc in cursor.description]
                        print(f"Columns: {', '.join(column_names)}")
                        print(f"Results: {len(results)} rows")
                        
                        # Display first 10 results
                        for i, row in enumerate(results[:10]):
                            print(f"  {i+1:2d}. {row}")
                        
                        if len(results) > 10:
                            print(f"  ... and {len(results) - 10} more rows")
                    else:
                        print("  No results found")
                        
            except Exception as e:
                print(f"  ❌ Error running query on {database_name}: {e}")
        
        print("\n" + "=" * 60 + "\n")
    
    def run_metrics_definition_queries(self):
        """Run queries on metrics_definition table (public schema)"""
        print("📋 METRICS DEFINITION QUERIES (Public Schema)")
        print("=" * 60)
        
        # Connect to each database with public schema
        public_clients = connect_to_specific_databases(config.databases, "public")
        
        # Run metrics definition queries
        queries = [
            ("Power Metrics", POWER_METRICS_QUERY, "Basic power metrics from metrics_definition"),
            ("All Power Metrics", ALL_POWER_METRICS, "Complete power metrics with all fields"),
            ("Power Metrics by Units", POWER_METRICS_BY_UNITS, "Power metrics grouped by units"),
            ("High Accuracy Power Metrics", HIGH_ACCURACY_POWER_METRICS, "Power metrics with >95% accuracy"),
            ("Power Metrics by Type", POWER_METRICS_BY_TYPE, "Power metrics grouped by type"),
            ("Power FQDD Info", POWER_FQDD_INFO, "FQDD information for power metrics")
        ]
        
        for query_name, query, description in queries:
            print(f"\n📊 {query_name}")
            print(f"   {description}")
            print("-" * 50)
            
            for database_name, client in public_clients.items():
                try:
                    print(f"\n🔍 {database_name.upper()}:")
                    
                    with client.db_connection.cursor() as cursor:
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        if results:
                            column_names = [desc[0] for desc in cursor.description]
                            print(f"  Columns: {', '.join(column_names)}")
                            print(f"  Results: {len(results)} rows")
                            
                            for i, row in enumerate(results[:5]):
                                print(f"    {i+1}. {row}")
                            
                            if len(results) > 5:
                                print(f"    ... and {len(results) - 5} more rows")
                        else:
                            print("  No results found")
                            
                except Exception as e:
                    print(f"  ❌ Error: {e}")
    
    def run_power_analysis_queries(self):
        """Run power analysis queries on idrac schema"""
        print("\n⚡ POWER ANALYSIS QUERIES (iDRAC Schema)")
        print("=" * 60)
        
        # Only run on h100 and zen4 (they have idrac schema)
        idrac_databases = ["h100", "zen4"]
        
        for database_name in idrac_databases:
            if database_name not in self.clients:
                continue
                
            client = self.clients[database_name]
            
            try:
                print(f"\n🔍 {database_name.upper()} POWER ANALYSIS:")
                print("-" * 40)
                
                # Node Power Comparison
                print("📊 Node Power Comparison (last hour):")
                with client.db_connection.cursor() as cursor:
                    cursor.execute(NODE_POWER_COMPARISON)
                    results = cursor.fetchall()
                    
                    if results:
                        column_names = [desc[0] for desc in cursor.description]
                        print(f"  Columns: {', '.join(column_names)}")
                        print(f"  Results: {len(results)} nodes")
                        
                        for i, row in enumerate(results[:5]):
                            print(f"    Node {row[0]}: Compute={row[1]:.1f}W, CPU={row[2]:.1f}W, GPU={row[3]:.1f}W")
                        
                        if len(results) > 5:
                            print(f"    ... and {len(results) - 5} more nodes")
                    else:
                        print("  No power data found")
                
                # Power Efficiency Metrics
                print("\n📊 Power Efficiency Analysis:")
                with client.db_connection.cursor() as cursor:
                    cursor.execute(POWER_EFFICIENCY_METRICS)
                    results = cursor.fetchall()
                    
                    if results:
                        for i, row in enumerate(results[:5]):
                            print(f"    Node {row[0]}: Total={row[1]:.1f}W, CPU={row[4]:.1f}%, GPU={row[5]:.1f}%")
                        
                        if len(results) > 5:
                            print(f"    ... and {len(results) - 5} more nodes")
                    else:
                        print("  No efficiency data found")
                
                # Recent Power Data
                print("\n📊 Recent Power Data (last 10 records):")
                recent_query = get_recent_power_data("computepower", 10)
                with client.db_connection.cursor() as cursor:
                    cursor.execute(recent_query)
                    results = cursor.fetchall()
                    
                    if results:
                        for i, row in enumerate(results):
                            print(f"    {row[0]} - Node {row[1]}: {row[2]:.1f}W")
                    else:
                        print("  No recent power data found")
                        
            except Exception as e:
                print(f"  ❌ Error analyzing {database_name}: {e}")
    
    def run_time_range_analysis(self):
        """Run time range analysis queries"""
        print("\n⏰ TIME RANGE ANALYSIS")
        print("=" * 60)
        
        # Set time range (last 24 hours)
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        
        print(f"📅 Time Range: {start_time} to {end_time}")
        
        for database_name in ["h100", "zen4"]:
            if database_name not in self.clients:
                continue
                
            client = self.clients[database_name]
            
            try:
                print(f"\n🔍 {database_name.upper()} - 24 Hour Power Summary:")
                print("-" * 40)
                
                summary_query = get_power_summary(
                    start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time.strftime("%Y-%m-%d %H:%M:%S")
                )
                
                with client.db_connection.cursor() as cursor:
                    cursor.execute(summary_query)
                    results = cursor.fetchall()
                    
                    if results:
                        for row in results:
                            print(f"  {row[1]}: {row[4]:.1f}W avg, {row[5]:.1f}W min, {row[6]:.1f}W max ({row[3]} data points)")
                    else:
                        print("  No power data found for time range")
                        
            except Exception as e:
                print(f"  ❌ Error in time range analysis for {database_name}: {e}")


def main():
    """Main function to run all queries"""
    print("🚀 H100 Power Query Runner")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print(f"🗄️  Available databases: {', '.join(config.databases)}")
    print()
    
    runner = MultiDatabaseQueryRunner()
    
    try:
        # Connect to all databases
        runner.connect_all_databases()
        
        # Run metrics definition queries (public schema)
        runner.run_metrics_definition_queries()
        
        # Run power analysis queries (idrac schema)
        runner.run_power_analysis_queries()
        
        # Run time range analysis
        runner.run_time_range_analysis()
        
        print("✅ All queries completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        # Disconnect from all databases
        runner.disconnect_all()
        print(f"📅 Finished at: {datetime.now()}")


if __name__ == "__main__":
    main()
