#!/usr/bin/env python3
"""
Connection pooling system for REPACSS Power Measurement
Manages database connections efficiently with pooling and health checks
"""

import threading
import time
from typing import Dict, Optional, List
from queue import Queue, Empty
from contextlib import contextmanager
import logging

from .client import REPACSSPowerClient, DatabaseConfig, SSHConfig
from .config.config import config

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Manages a pool of database connections for efficient resource usage"""
    
    def __init__(self, max_connections: int = 10, max_idle_time: int = 300):
        self.max_connections = max_connections
        self.max_idle_time = max_idle_time
        self._pools: Dict[str, Queue] = {}
        self._connection_info: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._cleanup_thread = None
        self._shutdown = False
        
        # Start cleanup thread
        self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """Start background thread for connection cleanup"""
        self._cleanup_thread = threading.Thread(target=self._cleanup_idle_connections, daemon=True)
        self._cleanup_thread.start()
    
    def _cleanup_idle_connections(self):
        """Clean up idle connections in background"""
        while not self._shutdown:
            try:
                time.sleep(60)  # Check every minute
                self._cleanup_old_connections()
            except Exception as e:
                logger.error(f"Error in connection cleanup: {e}")
    
    def _cleanup_old_connections(self):
        """Remove connections that have been idle too long"""
        current_time = time.time()
        
        with self._lock:
            for pool_key, pool in self._pools.items():
                temp_connections = []
                
                while not pool.empty():
                    try:
                        connection_info = pool.get_nowait()
                        if current_time - connection_info['last_used'] < self.max_idle_time:
                            temp_connections.append(connection_info)
                        else:
                            # Close old connection
                            try:
                                connection_info['client'].disconnect()
                            except Exception as e:
                                logger.warning(f"Error closing old connection: {e}")
                    except Empty:
                        break
                
                # Put back valid connections
                for conn_info in temp_connections:
                    pool.put(conn_info)
    
    def _get_pool_key(self, database: str, schema: str) -> str:
        """Generate pool key for database and schema combination"""
        return f"{database}_{schema}"
    
    def _create_connection(self, database: str, schema: str) -> REPACSSPowerClient:
        """Create a new database connection"""
        db_config = config.get_database_config(database, schema)
        ssh_config = config.get_ssh_config()
        
        client = REPACSSPowerClient(db_config, ssh_config, schema=db_config.schema)
        client.connect()
        
        return client
    
    def get_connection(self, database: str, schema: str = None) -> Optional[REPACSSPowerClient]:
        """Get a connection from the pool or create a new one"""
        if schema is None:
            schema = config.get_default_schema(database)
        
        pool_key = self._get_pool_key(database, schema)
        
        with self._lock:
            # Get or create pool for this database/schema combination
            if pool_key not in self._pools:
                self._pools[pool_key] = Queue()
                self._connection_info[pool_key] = {
                    'created': time.time(),
                    'total_connections': 0,
                    'active_connections': 0
                }
            
            pool = self._pools[pool_key]
            connection_info = self._connection_info[pool_key]
            
            # Try to get existing connection
            try:
                conn_info = pool.get_nowait()
                conn_info['last_used'] = time.time()
                connection_info['active_connections'] += 1
                logger.debug(f"Reusing connection for {pool_key}")
                return conn_info['client']
            except Empty:
                # Create new connection if under limit
                if connection_info['total_connections'] < self.max_connections:
                    try:
                        client = self._create_connection(database, schema)
                        connection_info['total_connections'] += 1
                        connection_info['active_connections'] += 1
                        logger.debug(f"Created new connection for {pool_key}")
                        return client
                    except Exception as e:
                        logger.error(f"Failed to create connection for {pool_key}: {e}")
                        return None
                else:
                    logger.warning(f"Connection pool full for {pool_key}")
                    return None
    
    def return_connection(self, client: REPACSSPowerClient, database: str, schema: str = None):
        """Return a connection to the pool"""
        if schema is None:
            schema = config.get_default_schema(database)
        
        pool_key = self._get_pool_key(database, schema)
        
        with self._lock:
            if pool_key in self._pools:
                pool = self._pools[pool_key]
                connection_info = self._connection_info[pool_key]
                
                # Check if connection is still valid
                try:
                    # Simple health check
                    with client.db_connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                    
                    # Return to pool
                    conn_info = {
                        'client': client,
                        'last_used': time.time(),
                        'database': database,
                        'schema': schema
                    }
                    pool.put(conn_info)
                    connection_info['active_connections'] -= 1
                    logger.debug(f"Returned connection to pool for {pool_key}")
                    
                except Exception as e:
                    # Connection is invalid, close it
                    logger.warning(f"Connection health check failed for {pool_key}: {e}")
                    try:
                        client.disconnect()
                    except Exception as close_error:
                        logger.warning(f"Error closing invalid connection: {close_error}")
                    
                    connection_info['total_connections'] -= 1
                    connection_info['active_connections'] -= 1
    
    def close_all_connections(self):
        """Close all connections in the pool"""
        with self._lock:
            for pool_key, pool in self._pools.items():
                while not pool.empty():
                    try:
                        conn_info = pool.get_nowait()
                        try:
                            conn_info['client'].disconnect()
                        except Exception as e:
                            logger.warning(f"Error closing connection: {e}")
                    except Empty:
                        break
                
                # Clear the pool
                while not pool.empty():
                    try:
                        pool.get_nowait()
                    except Empty:
                        break
            
            self._pools.clear()
            self._connection_info.clear()
        
        logger.info("All connections closed")
    
    def get_pool_status(self) -> Dict[str, Dict]:
        """Get status of all connection pools"""
        with self._lock:
            status = {}
            for pool_key, pool in self._pools.items():
                connection_info = self._connection_info[pool_key]
                status[pool_key] = {
                    'total_connections': connection_info['total_connections'],
                    'active_connections': connection_info['active_connections'],
                    'available_connections': pool.qsize(),
                    'created': connection_info['created']
                }
            return status
    
    def shutdown(self):
        """Shutdown the connection pool"""
        self._shutdown = True
        self.close_all_connections()
        
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)


# Global connection pool instance
_connection_pool: Optional[ConnectionPool] = None


def get_connection_pool() -> ConnectionPool:
    """Get the global connection pool instance"""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = ConnectionPool()
    return _connection_pool


@contextmanager
def get_pooled_connection(database: str, schema: str = None):
    """Context manager for getting and returning pooled connections"""
    pool = get_connection_pool()
    client = None
    
    try:
        client = pool.get_connection(database, schema)
        if client is None:
            raise ConnectionError(f"Failed to get connection for {database}/{schema}")
        yield client
    finally:
        if client:
            pool.return_connection(client, database, schema)


def close_all_pools():
    """Close all connection pools"""
    global _connection_pool
    if _connection_pool:
        _connection_pool.shutdown()
        _connection_pool = None
