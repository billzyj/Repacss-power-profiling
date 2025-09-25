"""
Unit tests for power conversion utilities
"""
import pytest
import pandas as pd
from datetime import datetime

# Import the functions we're testing
from src.utils.conversions import convert_power_series_to_watts


class TestPowerConversions:
    """Test power unit conversions"""
    
    def test_convert_watts_to_kwh(self):
        """Test conversion from watts to kWh"""
        # Test data: 1000W for 1 hour = 1 kWh
        power_data = pd.DataFrame({
            'timestamp': [datetime(2025, 1, 1, 10, 0, 0), datetime(2025, 1, 1, 11, 0, 0)],
            'power_watts': [1000, 1000]
        })
        
        result = convert_power_series_to_watts(power_data, 'W')
        
        # Should convert to kWh (1000W * 1h = 1kWh)
        assert result['energy_kwh'].iloc[0] == 1.0
    
    def test_convert_milliwatts_to_watts(self):
        """Test conversion from milliwatts to watts"""
        power_data = pd.DataFrame({
            'timestamp': [datetime(2025, 1, 1, 10, 0, 0)],
            'power_mw': [50000]  # 50,000 mW = 50W
        })
        
        result = convert_power_series_to_watts(power_data, 'mW')
        
        assert result['power_watts'].iloc[0] == 50.0
    
    def test_invalid_unit_raises_error(self):
        """Test that invalid units raise appropriate errors"""
        power_data = pd.DataFrame({
            'timestamp': [datetime(2025, 1, 1, 10, 0, 0)],
            'power': [1000]
        })
        
        with pytest.raises(ValueError, match="Unsupported unit"):
            convert_power_series_to_watts(power_data, 'invalid_unit')
    
    def test_empty_dataframe(self):
        """Test handling of empty dataframes"""
        empty_df = pd.DataFrame(columns=['timestamp', 'power_watts'])
        
        result = convert_power_series_to_watts(empty_df, 'W')
        
        assert len(result) == 0
        assert 'energy_kwh' in result.columns
