"""
Test fixtures for power data
"""
import pandas as pd
from datetime import datetime, timedelta
import pytest


@pytest.fixture
def sample_timestamps():
    """Generate sample timestamps for testing"""
    return pd.date_range(
        start=datetime(2025, 1, 1, 10, 0, 0),
        end=datetime(2025, 1, 1, 11, 0, 0),
        freq='1min'
    )


@pytest.fixture
def h100_power_data(sample_timestamps):
    """Sample H100 GPU node power data"""
    return pd.DataFrame({
        'timestamp': sample_timestamps,
        'hostname': ['rpg-93-1'] * len(sample_timestamps),
        'metric': ['CPUPower'] * len(sample_timestamps),
        'power_watts': [100 + i for i in range(len(sample_timestamps))],
        'units': ['W'] * len(sample_timestamps)
    })


@pytest.fixture
def zen4_power_data(sample_timestamps):
    """Sample ZEN4 CPU node power data"""
    return pd.DataFrame({
        'timestamp': sample_timestamps,
        'hostname': ['rpc-91-1'] * len(sample_timestamps),
        'metric': ['CPUPower'] * len(sample_timestamps),
        'power_watts': [80 + i for i in range(len(sample_timestamps))],
        'units': ['W'] * len(sample_timestamps)
    })


@pytest.fixture
def multi_metric_power_data(sample_timestamps):
    """Sample power data with multiple metrics"""
    data = []
    metrics = ['CPUPower', 'GPUPower', 'DRAMPwr', 'TotalFanPower']
    
    for metric in metrics:
        for i, ts in enumerate(sample_timestamps):
            data.append({
                'timestamp': ts,
                'hostname': 'rpg-93-1',
                'metric': metric,
                'power_watts': 50 + i + hash(metric) % 50,
                'units': 'W'
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def rack_power_data(sample_timestamps):
    """Sample power data for entire rack"""
    data = []
    nodes = ['rpg-93-1', 'rpg-93-2', 'rpg-93-3', 'rpg-93-4']
    metrics = ['CPUPower', 'GPUPower', 'SystemInputPower']
    
    for node in nodes:
        for metric in metrics:
            for i, ts in enumerate(sample_timestamps):
                data.append({
                    'timestamp': ts,
                    'hostname': node,
                    'metric': metric,
                    'power_watts': 100 + i + hash(node) % 100,
                    'units': 'W'
                })
    
    return pd.DataFrame(data)


@pytest.fixture
def energy_calculation_data():
    """Sample data for energy calculation testing"""
    return pd.DataFrame({
        'timestamp': [
            datetime(2025, 1, 1, 10, 0, 0),
            datetime(2025, 1, 1, 10, 1, 0),
            datetime(2025, 1, 1, 10, 2, 0),
            datetime(2025, 1, 1, 10, 3, 0),
            datetime(2025, 1, 1, 10, 4, 0)
        ],
        'power_watts': [100, 120, 110, 130, 115],
        'units': ['W', 'W', 'W', 'W', 'W']
    })


@pytest.fixture
def empty_power_data():
    """Empty power data for edge case testing"""
    return pd.DataFrame(columns=['timestamp', 'hostname', 'metric', 'power_watts', 'units'])


@pytest.fixture
def invalid_power_data():
    """Power data with invalid values for error testing"""
    return pd.DataFrame({
        'timestamp': [datetime(2025, 1, 1, 10, 0, 0)],
        'hostname': ['rpg-93-1'],
        'metric': ['CPUPower'],
        'power_watts': [-50],  # Negative power (invalid)
        'units': ['W']
    })


@pytest.fixture
def mixed_unit_power_data():
    """Power data with mixed units for conversion testing"""
    return pd.DataFrame({
        'timestamp': [
            datetime(2025, 1, 1, 10, 0, 0),
            datetime(2025, 1, 1, 10, 1, 0),
            datetime(2025, 1, 1, 10, 2, 0)
        ],
        'power_watts': [1000, 50000, 2.5],  # 1kW, 50kW, 2.5W
        'units': ['W', 'mW', 'kW']
    })
