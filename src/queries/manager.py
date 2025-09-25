#!/usr/bin/env python3
"""
Query management system for REPACSS Power Measurement
Provides unified query interface with validation and optimization
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import re
import logging

from .compute.idrac import get_compute_metrics_with_joins
from .infra.irc_pdu import get_irc_metrics_with_joins, get_pdu_metrics_with_joins
from database.connection_pool import get_pooled_connection

logger = logging.getLogger(__name__)


class QueryManager:
    """Manages database queries with validation and optimization"""
    
    def __init__(self, database: str, schema: str = None):
        self.database = database
        self.schema = schema
        self._query_cache: Dict[str, str] = {}
        self._result_cache: Dict[str, pd.DataFrame] = {}
    
    def get_power_metrics(self, hostname: str, start_time: datetime = None, 
                         end_time: datetime = None, limit: int = 100) -> pd.DataFrame:
        """
        Get power metrics for a specific hostname with unified interface.
        
        Args:
            hostname: Node hostname
            start_time: Start timestamp (optional)
            end_time: End timestamp (optional)
            limit: Maximum number of records (default: 100)
        
        Returns:
            DataFrame with power metrics
        """
        try:
            # Determine node type and get appropriate query function
            node_type, query_func, db, schema = self._get_node_type_and_query_func(hostname)
            
            # Get metrics list for this node type
            metrics = self._get_metrics_for_node_type(node_type, db, schema)
            
            # Execute queries for all metrics
            all_data = []
            for metric in metrics:
                try:
                    if node_type in ['pdu']:
                        # PDU doesn't use metric_id parameter
                        query = query_func(hostname, start_time, end_time)
                    else:
                        # IRC and compute nodes use metric_id
                        query = query_func(metric, hostname, start_time, end_time)
                    
                    # Execute query with connection pooling
                    with get_pooled_connection(db, schema) as client:
                        df = pd.read_sql_query(query, client.db_connection)
                        if not df.empty:
                            df['metric'] = metric
                            all_data.append(df)
                
                except Exception as e:
                    logger.warning(f"Error querying metric {metric} for {hostname}: {e}")
                    continue
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                return combined_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error getting power metrics for {hostname}: {e}")
            return pd.DataFrame()
    
    def get_metrics_definition(self, database: str = None, schema: str = None) -> pd.DataFrame:
        """
        Get metrics definition from public schema.
        
        Args:
            database: Database name (optional, uses instance database)
            schema: Schema name (optional, uses 'public')
        
        Returns:
            DataFrame with metrics definition
        """
        db = database or self.database
        schema = schema or 'public'
        
        query = """
        SELECT 
            metric_id,
            metric_name,
            description,
            metric_data_type,
            units,
            accuracy,
            sensing_interval
        FROM public.metrics_definition 
        ORDER BY metric_name
        """
        
        try:
            with get_pooled_connection(db, schema) as client:
                return pd.read_sql_query(query, client.db_connection)
        except Exception as e:
            logger.error(f"Error getting metrics definition: {e}")
            return pd.DataFrame()
    
    def get_power_metrics_definition(self, database: str = None, schema: str = None) -> pd.DataFrame:
        """
        Get power-related metrics definition.
        
        Args:
            database: Database name (optional)
            schema: Schema name (optional)
        
        Returns:
            DataFrame with power metrics definition
        """
        db = database or self.database
        schema = schema or 'public'
        
        query = """
        SELECT 
            metric_id,
            metric_name,
            description,
            metric_data_type,
            units,
            accuracy,
            sensing_interval
        FROM public.metrics_definition 
        WHERE units IN ('mW', 'W', 'kW') OR metric_name LIKE '%Power%'
        ORDER BY metric_name
        """
        
        try:
            with get_pooled_connection(db, schema) as client:
                return pd.read_sql_query(query, client.db_connection)
        except Exception as e:
            logger.error(f"Error getting power metrics definition: {e}")
            return pd.DataFrame()
    
    def execute_custom_query(self, query: str, database: str = None, schema: str = None) -> pd.DataFrame:
        """
        Execute a custom SQL query with validation.
        
        Args:
            query: SQL query string
            database: Database name (optional)
            schema: Schema name (optional)
        
        Returns:
            Query results as DataFrame
        """
        # Validate query
        if not self._validate_query(query):
            raise ValueError("Invalid or potentially dangerous query")
        
        db = database or self.database
        schema = schema or self.schema
        
        try:
            with get_pooled_connection(db, schema) as client:
                return pd.read_sql_query(query, client.db_connection)
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            return pd.DataFrame()
    
    def get_database_info(self, database: str = None) -> Dict[str, Any]:
        """
        Get database information and statistics.
        
        Args:
            database: Database name (optional)
        
        Returns:
            Dictionary with database information
        """
        db = database or self.database
        
        try:
            with get_pooled_connection(db, 'public') as client:
                # Get database version
                version_query = "SELECT version()"
                version_result = pd.read_sql_query(version_query, client.db_connection)
                
                # Get table count
                tables_query = """
                SELECT COUNT(*) as table_count 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                """
                tables_result = pd.read_sql_query(tables_query, client.db_connection)
                
                # Get schema information
                schemas_query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
                """
                schemas_result = pd.read_sql_query(schemas_query, client.db_connection)
                
                return {
                    'database': db,
                    'version': version_result.iloc[0]['version'] if not version_result.empty else 'Unknown',
                    'table_count': tables_result.iloc[0]['table_count'] if not tables_result.empty else 0,
                    'schemas': schemas_result['schema_name'].tolist() if not schemas_result.empty else []
                }
                
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {'database': db, 'error': str(e)}
    
    def _get_node_type_and_query_func(self, hostname: str):
        """Determine node type and return appropriate query function"""
        hostname_mapping = {
            'pdu': ('pdu', get_pdu_metrics_with_joins, 'infra', 'pdu'),
            'irc': ('irc', get_irc_metrics_with_joins, 'infra', 'irc'),
            'rpg': ('h100', get_compute_metrics_with_joins, 'h100', 'idrac'),
            'rpc': ('zen4', get_compute_metrics_with_joins, 'zen4', 'idrac')
        }
        
        for prefix, (node_type, query_func, database, schema) in hostname_mapping.items():
            if hostname.startswith(prefix):
                return node_type, query_func, database, schema
        
        raise ValueError(f"Invalid hostname: {hostname}")
    
    def _get_metrics_for_node_type(self, node_type: str, database: str, schema: str) -> List[str]:
        """Get metrics list for a specific node type"""
        if node_type == 'pdu':
            return ['pdu']
        elif node_type == 'irc':
            return ['CompressorPower', 'CondenserFanPower', 'CoolDemand', 'CoolOutput', 
                   'TotalAirSideCoolingDemand', 'TotalSensibleCoolingPower']
        else:  # compute nodes
            return self._get_compute_power_metrics(database, schema)
    
    def _get_compute_power_metrics(self, database: str, schema: str) -> List[str]:
        """Get power metrics for compute nodes from the database"""
        try:
            with get_pooled_connection(database, 'public') as client:
                query = """
                SELECT metric_id
                FROM public.metrics_definition 
                WHERE units IN ('mW', 'W', 'kW')
                ORDER BY metric_id
                """
                df = pd.read_sql_query(query, client.db_connection)
                return df['metric_id'].tolist()
        except Exception as e:
            logger.error(f"Error getting compute power metrics: {e}")
            return []
    
    def _validate_query(self, query: str) -> bool:
        """
        Validate SQL query for security and syntax.
        
        Args:
            query: SQL query string
        
        Returns:
            True if query is valid, False otherwise
        """
        # Convert to lowercase for validation
        query_lower = query.lower().strip()
        
        # Check for dangerous operations
        dangerous_patterns = [
            r'\bdrop\b',
            r'\bdelete\b',
            r'\binsert\b',
            r'\bupdate\b',
            r'\balter\b',
            r'\bcreate\b',
            r'\btruncate\b',
            r'\bexec\b',
            r'\bexecute\b',
            r'\bunion\b.*\bselect\b',
            r'\b--',
            r'/\*.*\*/'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_lower):
                logger.warning(f"Potentially dangerous query detected: {pattern}")
                return False
        
        # Check if it's a SELECT query
        if not query_lower.startswith('select'):
            logger.warning("Query must start with SELECT")
            return False
        
        return True
    
    def get_query_performance_stats(self, query: str, database: str = None, schema: str = None) -> Dict[str, Any]:
        """
        Get query performance statistics.
        
        Args:
            query: SQL query string
            database: Database name (optional)
            schema: Schema name (optional)
        
        Returns:
            Dictionary with performance statistics
        """
        db = database or self.database
        schema = schema or self.schema
        
        try:
            with get_pooled_connection(db, schema) as client:
                # Add EXPLAIN ANALYZE to get performance stats
                explain_query = f"EXPLAIN ANALYZE {query}"
                
                with client.db_connection.cursor() as cursor:
                    cursor.execute(explain_query)
                    explain_result = cursor.fetchall()
                
                # Parse explain result (simplified)
                execution_time = None
                for row in explain_result:
                    if 'Execution Time:' in str(row):
                        # Extract execution time
                        time_match = re.search(r'Execution Time: ([\d.]+) ms', str(row))
                        if time_match:
                            execution_time = float(time_match.group(1))
                
                return {
                    'query': query,
                    'execution_time_ms': execution_time,
                    'explain_result': [str(row) for row in explain_result]
                }
                
        except Exception as e:
            logger.error(f"Error getting query performance stats: {e}")
            return {'query': query, 'error': str(e)}
    
    def clear_cache(self):
        """Clear query and result caches"""
        self._query_cache.clear()
        self._result_cache.clear()
        logger.info("Query caches cleared")
