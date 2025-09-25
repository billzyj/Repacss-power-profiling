#!/usr/bin/env python3
"""
Test and validation CLI commands
"""

import click
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database.database import connect_to_database, disconnect_all
from database.config.config import config


@click.command()
@click.option('--database', 
              type=click.Choice(['h100', 'zen4', 'infra']), 
              default='h100',
              help='Database to test')
@click.option('--schema', 
              help='Specific schema to test (optional)')
def connection(database, schema):
    """
    Test database connection and SSH tunnel.
    
    Examples:
        # Test default H100 connection
        python -m src.cli test connection
        
        # Test specific database and schema
        python -m src.cli test connection --database infra --schema pdu
    """
    
    click.echo(f"🔌 Testing connection to {database} database...")
    if schema:
        click.echo(f"📋 Schema: {schema}")
    click.echo()
    
    try:
        # Test connection
        client = connect_to_database(database, schema)
        
        if not client:
            click.echo("❌ Failed to connect to database")
            sys.exit(1)
        
        click.echo("✅ Connection successful!")
        
        # Test basic query
        click.echo("🔍 Testing basic query...")
        with client.db_connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            click.echo(f"📊 Database version: {version[0]}")
        
        # Test schema-specific query
        if schema:
            click.echo(f"🔍 Testing {schema} schema...")
            with client.db_connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema}';")
                table_count = cursor.fetchone()
                click.echo(f"📋 Tables in {schema} schema: {table_count[0]}")
        
        click.echo("✅ All tests passed!")
        
    except Exception as e:
        click.echo(f"❌ Connection test failed: {e}")
        sys.exit(1)
    finally:
        disconnect_all()


@click.command()
@click.option('--all', 
              is_flag=True,
              help='Test all databases')
def databases(all):
    """
    Test connections to all configured databases.
    
    Examples:
        # Test all databases
        python -m src.cli test databases --all
        
        # Test specific database (use test connection command)
        python -m src.cli test connection --database h100
    """
    
    if not all:
        click.echo("Use --all flag to test all databases, or use 'test connection' for specific database")
        return
    
    click.echo("🔌 Testing connections to all databases...")
    click.echo()
    
    success_count = 0
    total_count = len(config.databases)
    
    try:
        for database in config.databases:
            try:
                click.echo(f"🔍 Testing {database}...")
                client = connect_to_database(database)
                
                if client:
                    click.echo(f"✅ {database}: Connected successfully")
                    success_count += 1
                else:
                    click.echo(f"❌ {database}: Connection failed")
                    
            except Exception as e:
                click.echo(f"❌ {database}: Error - {e}")
        
        click.echo()
        click.echo(f"📊 Results: {success_count}/{total_count} databases connected successfully")
        
        if success_count == total_count:
            click.echo("✅ All database connections successful!")
        else:
            click.echo("⚠️  Some database connections failed")
            sys.exit(1)
    
    finally:
        disconnect_all()


@click.command()
def config():
    """
    Validate configuration settings.
    
    Examples:
        # Validate configuration
        python -m src.cli test config
    """
    
    click.echo("⚙️  Validating configuration...")
    click.echo()
    
    # Test configuration values
    issues = []
    
    # Check database settings
    if not config.db_host:
        issues.append("Database host not configured")
    if not config.db_user:
        issues.append("Database user not configured")
    if not config.db_password:
        issues.append("Database password not configured")
    
    # Check SSH settings
    if not config.ssh_hostname:
        issues.append("SSH hostname not configured")
    if not config.ssh_username:
        issues.append("SSH username not configured")
    if not config.ssh_private_key_path:
        issues.append("SSH private key path not configured")
    
    # Check SSH key file
    if config.ssh_private_key_path and not os.path.exists(config.ssh_private_key_path):
        issues.append(f"SSH private key file not found: {config.ssh_private_key_path}")
    
    # Display results
    if issues:
        click.echo("❌ Configuration issues found:")
        for issue in issues:
            click.echo(f"  - {issue}")
        click.echo()
        click.echo("Please update your configuration in src/core/config.py")
        sys.exit(1)
    else:
        click.echo("✅ Configuration validation passed!")
        click.echo()
        click.echo("📋 Configuration summary:")
        click.echo(f"  - Database host: {config.db_host}")
        click.echo(f"  - Database user: {config.db_user}")
        click.echo(f"  - SSH hostname: {config.ssh_hostname}")
        click.echo(f"  - SSH username: {config.ssh_username}")
        click.echo(f"  - Available databases: {', '.join(config.databases)}")
