# REPACSS Power Measurement

Power measurement from out-of-band and in-band solutions for the REPACSS cluster.

## Overview

This project provides a Python client to connect to the REPACSS TimescaleDB and query power-related metrics from iDRAC (Integrated Dell Remote Access Controller) and infrastructure monitoring systems. The client supports multiple databases, each containing different schemas with power monitoring data.

## Features

- **Secure SSH tunnel connection** to TimescaleDB using `sshtunnel`
- **Multi-database support** for different cluster databases
- Query power consumption, temperature, and utilization metrics from various schemas
- Support for time-range queries and aggregations
- Cluster-wide and node-specific summaries
- Custom SQL query execution
- Robust error handling and connection management
- Real-time power monitoring across multiple nodes
- Infrastructure monitoring (PDU, IRC, compressors, airflow)
- **Excel report generation** with comprehensive power metrics from all databases

## Quick Start

### 1. Installation

```bash
git clone <repository-url>
cd repacss-power-measurement
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
python setup.py
```

### 2. Configuration

The setup script automatically creates the configuration file and adds it to `.gitignore`. You just need to edit the generated file with your credentials:

```bash
# Edit src/core/config.py with your database and SSH credentials
```

### 3. Test Connection

```bash
python src/examples/test_connection.py
```

### 4. Generate Power Metrics Report

```bash
python src/scripts/run_public_queries.py
```

This will create an Excel file with comprehensive power metrics from all databases.

## Basic Usage

```python
from core.client import REPACSSPowerClient, DatabaseConfig, SSHConfig
from core.config import config

# Create client
client = REPACSSPowerClient(
    DatabaseConfig(**config.get_database_config("h100")),
    SSHConfig(**config.get_ssh_config())
)

try:
    client.connect()
    
    # Get recent power metrics
    metrics = client.get_computepower_metrics(limit=10)
    for metric in metrics:
        print(f"Node {metric['nodeid']}: {metric['value']:.1f}W")
        
finally:
    client.disconnect()
```

## Documentation

- **[Usage Guide](docs/USAGE_GUIDE.md)** - Detailed usage examples, Excel reporting, and troubleshooting
- **[Project Structure](docs/PROJECT_STRUCTURE.md)** - Technical architecture and development details

## Dependencies

- `psycopg2-binary>=2.9.0` - PostgreSQL adapter
- `pandas>=1.5.0` - Data manipulation and Excel export
- `openpyxl>=3.0.0` - Excel file generation
- `paramiko==3.3.1` - SSH protocol implementation
- `sshtunnel>=0.4.0` - SSH tunnel management

## License

[License information]
