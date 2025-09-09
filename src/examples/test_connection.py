#!/usr/bin/env python3
"""
Test script to verify SSH tunnel and database connection
"""

import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.client import REPACSSPowerClient, DatabaseConfig, SSHConfig
from core.config import config

def test_connection():
    """Test the SSH tunnel and database connection"""
    
    print("Testing REPACSS Power Client Connection...")
    print("=" * 50)
    
    # Create configuration objects
    db_config_dict = config.get_database_config()
    ssh_config_dict = config.get_ssh_config()
    
    db_config = DatabaseConfig(**db_config_dict)
    ssh_config = SSHConfig(**ssh_config_dict)
    
    print(f"SSH Configuration:")
    print(f"  Host: {ssh_config.hostname}")
    print(f"  Port: {ssh_config.port}")
    print(f"  Username: {ssh_config.username}")
    print(f"  Key Path: {ssh_config.private_key_path}")
    print(f"  Key Exists: {os.path.exists(ssh_config.private_key_path)}")
    
    print(f"\nDatabase Configuration:")
    print(f"  Host: {db_config.host}")
    print(f"  Port: {db_config.port}")
    print(f"  Database: {db_config.database}")
    print(f"  Schema: {db_config.schema}")
    print(f"  Username: {db_config.username}")
    print(f"  Password Set: {'Yes' if db_config.password else 'No'}")
    
    # Create client
    client = REPACSSPowerClient(db_config, ssh_config, schema=db_config.schema)
    
    try:
        print(f"\n1. Testing SSH tunnel...")
        client.connect()
        print("✓ SSH tunnel and database connection successful!")
        
        print(f"\n2. Testing database query...")
        with client.db_connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"✓ Database version: {version[0]}")
        
        print(f"\n3. Testing table existence...")
        with client.db_connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{db_config.schema}' 
                AND (table_name LIKE '%power%' OR table_name LIKE '%idrac%' OR table_name LIKE '%pdu%')
            """)
            tables = cursor.fetchall()
            if tables:
                print(f"✓ Found tables in '{db_config.schema}' schema:")
                for table in tables:
                    print(f"  - {table[0]}")
            else:
                print(f"⚠ No power-related tables found in '{db_config.schema}' schema")
        
        print(f"\n4. Testing custom query...")
        try:
            # Try to get some basic info about the database
            with client.db_connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user, version();")
                info = cursor.fetchone()
                print(f"✓ Database: {info[0]}")
                print(f"✓ User: {info[1]}")
                print(f"✓ PostgreSQL Version: {info[2]}")
        except Exception as e:
            print(f"⚠ Query test failed: {e}")
        
        print(f"\n✓ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print(f"\nTroubleshooting tips:")
        print(f"1. Verify SSH key permissions: chmod 600 {ssh_config.private_key_path}")
        print(f"2. Test SSH manually: ssh -i {ssh_config.private_key_path} {ssh_config.username}@{ssh_config.hostname}")
        print(f"3. Check if database password is set in environment: DB_PASSWORD")
        print(f"4. Verify database credentials and SSL settings")
        return False
    
    finally:
        client.disconnect()
        print(f"\n✓ Disconnected from database")
    
    return True

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1) 