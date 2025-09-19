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

## Power Analysis with Energy Calculation

The power analysis functions provide comprehensive energy consumption tracking with boundary-aware calculations:

```python
from core.power_utils import power_analysis, multi_node_power_analysis

# Single node analysis
df, energy_dict = power_analysis("rpg-01", "2025-01-01 23:00:00", "2025-01-01 23:30:00")

# Multi-node analysis
results = multi_node_power_analysis(
    ["rpg-01", "pdu-91-1", "irc-91-5"], 
    "2025-01-01 23:00:00", 
    "2025-01-01 23:30:00"
)
```

### Cumulative Energy Logic

The system calculates energy consumption with proper boundary handling:

1. **Boundary Rows**: When query results don't include exact start/end times, boundary rows are added
2. **First Row**: Cumulative energy starts at 0.0
3. **Second Row**: Uses first power reading as average (no previous reading to average with)
4. **Remaining Rows**: Uses trapezoidal rule (average of current and previous power readings)

### Example Output

```python
# Query: 23:00:00 - 23:30:00
# Data: 23:00:01 - 23:29:59

timestamp           metric        value  power_w  energy_interval_kwh  cumulative_energy_kwh
23:00:00           systempower   150.0   150.0    0.0                  0.0                    # Start boundary
23:00:01           systempower   150.0   150.0    0.000042             0.000042               # First interval
23:00:31           systempower   155.0   155.0    0.00127              0.00131                # Trapezoidal
23:01:01           systempower   148.0   148.0    0.00126              0.00257                # Trapezoidal
23:01:31           systempower   152.0   152.0    0.00125              0.00382                # Trapezoidal
...
23:29:59           systempower   152.0   152.0    0.00125              0.04567                # Last data
23:30:00           systempower   152.0   152.0    0.0                  0.04567                # End boundary
```

### DataFrame Columns

The analysis returns a DataFrame with the following columns:

- `timestamp`: DateTime of the power reading
- `hostname`: Node hostname
- `value`: Original power reading (in original units)
- `units`: Unit from metrics_definition table (mW, W, kW)
- `metric`: Metric ID (table name)
- `power_w`: Power converted to Watts
- `time_diff_seconds`: Time difference from previous reading
- `avg_power_w`: Average power between current and previous readings
- `energy_interval_kwh`: Energy consumed in this time interval
- `cumulative_energy_kwh`: Total energy consumed up to this timestamp

### Node Type Support

- **Compute Nodes** (rpg-*, rpc-*): Automatically queries all power-related metrics from database
- **PDU Nodes** (pdu-*): Uses predefined PDU power metrics
- **IRC Nodes** (irc-*): Uses predefined IRC power metrics (CompressorPower, CondenserFanPower, etc.)

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
