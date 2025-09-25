"""
Unit tests for energy calculation utilities
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.analysis.energy import EnergyCalculator


class TestEnergyCalculation:
    """Test energy calculation functions"""
    
    def test_calculate_energy_for_hostname(self):
        """Test energy calculation for a specific hostname"""
        # Sample power data
        timestamps = pd.date_range(
            start=datetime(2025, 1, 1, 10, 0, 0),
            end=datetime(2025, 1, 1, 11, 0, 0),
            freq='1min'
        )
        
        power_data = pd.DataFrame({
            'timestamp': timestamps,
            'power_watts': [100] * len(timestamps),
            'units': ['W'] * len(timestamps)
        })
        
        # Test energy calculation
        from src.analysis.energy import compute_energy_kwh_for_hostname
        
        energy_kwh = compute_energy_kwh_for_hostname(
            power_data, 'W', 'test-node',
            '2025-01-01 10:00:00', '2025-01-01 11:00:00'
        )
        
        # 100W for 1 hour = 0.1 kWh
        assert abs(energy_kwh - 0.1) < 0.001
    
    def test_energy_calculation_with_varying_power(self):
        """Test energy calculation with varying power consumption"""
        timestamps = pd.date_range(
            start=datetime(2025, 1, 1, 10, 0, 0),
            end=datetime(2025, 1, 1, 10, 2, 0),
            freq='1min'
        )
        
        # Varying power: 100W, 200W, 300W
        power_data = pd.DataFrame({
            'timestamp': timestamps,
            'power_watts': [100, 200, 300],
            'units': ['W', 'W', 'W']
        })
        
        from src.analysis.energy import compute_energy_kwh_for_hostname
        
        energy_kwh = compute_energy_kwh_for_hostname(
            power_data, 'W', 'test-node',
            '2025-01-01 10:00:00', '2025-01-01 10:02:00'
        )
        
        # Average power: (100+200+300)/3 = 200W for 2 minutes = 0.0067 kWh
        expected_energy = (200 * 2 * 60) / 3600000  # Convert to kWh
        assert abs(energy_kwh - expected_energy) < 0.001
    
    def test_energy_calculation_edge_cases(self):
        """Test energy calculation edge cases"""
        # Empty dataframe
        empty_df = pd.DataFrame(columns=['timestamp', 'power_watts', 'units'])
        
        from src.analysis.energy import compute_energy_kwh_for_hostname
        
        energy_kwh = compute_energy_kwh_for_hostname(
            empty_df, 'W', 'test-node',
            '2025-01-01 10:00:00', '2025-01-01 11:00:00'
        )
        
        assert energy_kwh == 0.0
    
    def test_energy_calculation_with_different_units(self):
        """Test energy calculation with different power units"""
        timestamps = pd.date_range(
            start=datetime(2025, 1, 1, 10, 0, 0),
            end=datetime(2025, 1, 1, 11, 0, 0),
            freq='1min'
        )
        
        # Test with milliwatts
        power_data_mw = pd.DataFrame({
            'timestamp': timestamps,
            'power_watts': [100000] * len(timestamps),  # 100,000 mW = 100W
            'units': ['mW'] * len(timestamps)
        })
        
        from src.analysis.energy import compute_energy_kwh_for_hostname
        
        energy_kwh = compute_energy_kwh_for_hostname(
            power_data_mw, 'mW', 'test-node',
            '2025-01-01 10:00:00', '2025-01-01 11:00:00'
        )
        
        # 100W for 1 hour = 0.1 kWh
        assert abs(energy_kwh - 0.1) < 0.001
