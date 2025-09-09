"""
Configuration template for REPACSS Power Measurement Client
Copy this file to config.py and fill in your actual values
"""

import os
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class Config:
    """Configuration for database and SSH connection"""
    
    # Database settings (these are the remote database settings)
    db_host: str = "localhost"  # This is the remote database host from SSH perspective
    db_port: int = 5432
    db_default_name: str = "h100"  # Default database name (h100, zen4, or infra)
    db_user: str = "your_database_user"
    db_password: str = "your_database_password"  # Database password
    db_ssl_mode: str = "prefer"
    
    # SSH tunnel settings
    ssh_hostname: str = "your.ssh.host.com"
    ssh_port: int = 22
    ssh_username: str = "your_ssh_username"
    ssh_private_key_path: str = "/path/to/your/private/key"
    ssh_passphrase: str = ""  # SSH key passphrase (empty if no passphrase)
    ssh_keepalive_interval: int = 60
    
    # Database schemas configuration
    _database_schemas: dict = None
    
    def __post_init__(self):
        if self._database_schemas is None:
            self._database_schemas = {
                "h100": {
                    "schemas": ["public", "idrac", "slurm"],
                    "default_schema": "idrac"
                },
                "zen4": {
                    "schemas": ["public", "idrac", "slurm"],
                    "default_schema": "idrac"
                },
                "infra": {
                    "schemas": ["public", "irc", "pdu"],
                    "default_schema": "pdu"
                }
            }
    
    @property
    def databases(self) -> List[str]:
        """Get list of available databases"""
        return list(self._database_schemas.keys())
    
    def get_database_config(self, database_name: str = None, schema: str = None) -> dict:
        """Get database configuration for a specific database and schema"""
        if database_name is None:
            database_name = self.db_default_name
        
        if schema is None:
            schema = self._database_schemas.get(database_name, {}).get("default_schema", "public")
        
        return {
            'host': self.db_host,
            'port': self.db_port,
            'database': database_name,
            'username': self.db_user,
            'password': self.db_password,
            'ssl_mode': self.db_ssl_mode,
            'schema': schema
        }
    
    def get_available_schemas(self, database_name: str) -> List[str]:
        """Get available schemas for a specific database"""
        return self._database_schemas.get(database_name, {}).get("schemas", ["public"])
    
    def get_default_schema(self, database_name: str) -> str:
        """Get default schema for a specific database"""
        return self._database_schemas.get(database_name, {}).get("default_schema", "public")
    
    def get_ssh_config(self) -> dict:
        """Get SSH configuration"""
        return {
            'hostname': self.ssh_hostname,
            'port': self.ssh_port,
            'username': self.ssh_username,
            'private_key_path': self.ssh_private_key_path,
            'passphrase': self.ssh_passphrase,
            'keepalive_interval': self.ssh_keepalive_interval
        }


# Default configuration
config = Config()