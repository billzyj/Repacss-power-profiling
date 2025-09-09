#!/usr/bin/env python3
"""
Database connection utilities for REPACSS Power Measurement
Provides shared functionality for connecting to multiple databases
"""

from typing import Dict, List, Optional
from .client import REPACSSPowerClient, DatabaseConfig, SSHConfig
from .config import config


class DatabaseConnectionManager:
    """Manages multiple database connections with SSH tunnels"""
    
    def __init__(self):
        self.clients: Dict[str, REPACSSPowerClient] = {}
        self.connected_databases: List[str] = []
    
    def create_client_for_database(self, database_name: str, schema: str = None) -> REPACSSPowerClient:
        """Create a client instance for a specific database and schema"""
        db_config_dict = config.get_database_config(database_name, schema)
        ssh_config_dict = config.get_ssh_config()
        
        db_config = DatabaseConfig(**db_config_dict)
        ssh_config = SSHConfig(**ssh_config_dict)
        
        return REPACSSPowerClient(db_config, ssh_config, schema=db_config.schema)
    
    def connect_to_database(self, database_name: str, schema: str = None) -> Optional[REPACSSPowerClient]:
        """Connect to a specific database and return the client"""
        try:
            client = self.create_client_for_database(database_name, schema)
            client.connect()
            
            # Store the client
            key = f"{database_name}_{schema}" if schema else database_name
            self.clients[key] = client
            self.connected_databases.append(database_name)
            
            print(f"✓ Connected to {database_name} ({schema or 'default'} schema)")
            return client
            
        except Exception as e:
            print(f"❌ Failed to connect to {database_name}: {e}")
            return None
    
    def connect_to_all_databases(self, schema: str = None) -> Dict[str, REPACSSPowerClient]:
        """Connect to all available databases"""
        print("🔌 Connecting to all databases...")
        
        connected_clients = {}
        
        for database_name in config.databases:
            client = self.connect_to_database(database_name, schema)
            if client:
                connected_clients[database_name] = client
        
        print(f"✓ Connected to {len(connected_clients)} databases")
        return connected_clients
    
    def connect_to_specific_databases(self, database_names: List[str], schema: str = None) -> Dict[str, REPACSSPowerClient]:
        """Connect to specific databases"""
        print(f"🔌 Connecting to specific databases: {', '.join(database_names)}")
        
        connected_clients = {}
        
        for database_name in database_names:
            if database_name in config.databases:
                client = self.connect_to_database(database_name, schema)
                if client:
                    connected_clients[database_name] = client
            else:
                print(f"⚠️  Database '{database_name}' not found in config")
        
        print(f"✓ Connected to {len(connected_clients)} databases")
        return connected_clients
    
    def disconnect_from_database(self, database_name: str, schema: str = None):
        """Disconnect from a specific database"""
        key = f"{database_name}_{schema}" if schema else database_name
        
        if key in self.clients:
            try:
                self.clients[key].disconnect()
                del self.clients[key]
                if database_name in self.connected_databases:
                    self.connected_databases.remove(database_name)
                print(f"✓ Disconnected from {database_name}")
            except Exception as e:
                print(f"❌ Error disconnecting from {database_name}: {e}")
    
    def disconnect_all(self):
        """Disconnect from all databases"""
        print("🔌 Disconnecting from all databases...")
        
        for key, client in list(self.clients.items()):
            try:
                client.disconnect()
                print(f"  ✓ Disconnected from {key}")
            except Exception as e:
                print(f"  ❌ Error disconnecting from {key}: {e}")
        
        self.clients.clear()
        self.connected_databases.clear()
        print("✓ All connections closed")
    
    def get_client(self, database_name: str, schema: str = None) -> Optional[REPACSSPowerClient]:
        """Get a connected client for a specific database"""
        key = f"{database_name}_{schema}" if schema else database_name
        return self.clients.get(key)
    
    def get_all_clients(self) -> Dict[str, REPACSSPowerClient]:
        """Get all connected clients"""
        return self.clients.copy()
    
    def is_connected(self, database_name: str, schema: str = None) -> bool:
        """Check if connected to a specific database"""
        key = f"{database_name}_{schema}" if schema else database_name
        return key in self.clients
    
    def get_connected_databases(self) -> List[str]:
        """Get list of connected database names"""
        return self.connected_databases.copy()


# Global connection manager instance
connection_manager = DatabaseConnectionManager()


def get_connection_manager() -> DatabaseConnectionManager:
    """Get the global connection manager instance"""
    return connection_manager


def create_client_for_database(database_name: str, schema: str = None) -> REPACSSPowerClient:
    """Create a client instance for a specific database and schema"""
    return connection_manager.create_client_for_database(database_name, schema)


def connect_to_database(database_name: str, schema: str = None) -> Optional[REPACSSPowerClient]:
    """Connect to a specific database and return the client"""
    return connection_manager.connect_to_database(database_name, schema)


def connect_to_all_databases(schema: str = None) -> Dict[str, REPACSSPowerClient]:
    """Connect to all available databases"""
    return connection_manager.connect_to_all_databases(schema)


def connect_to_specific_databases(database_names: List[str], schema: str = None) -> Dict[str, REPACSSPowerClient]:
    """Connect to specific databases"""
    return connection_manager.connect_to_specific_databases(database_names, schema)


def disconnect_all():
    """Disconnect from all databases"""
    connection_manager.disconnect_all()


def get_client(database_name: str, schema: str = None) -> Optional[REPACSSPowerClient]:
    """Get a connected client for a specific database"""
    return connection_manager.get_client(database_name, schema)


def get_all_clients() -> Dict[str, REPACSSPowerClient]:
    """Get all connected clients"""
    return connection_manager.get_all_clients()


def is_connected(database_name: str, schema: str = None) -> bool:
    """Check if connected to a specific database"""
    return connection_manager.is_connected(database_name, schema)


def get_connected_databases() -> List[str]:
    """Get list of connected database names"""
    return connection_manager.get_connected_databases()
