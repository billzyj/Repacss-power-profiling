"""
Metric definitions for REPACSS Power Measurement
"""

# IRC (Infrastructure) power metrics
IRC_POWER_METRICS = [
    'CompressorPower', 
    'CondenserFanPower', 
    'CoolDemand', 
    'CoolOutput', 
    'TotalAirSideCoolingDemand', 
    'TotalSensibleCoolingPower'
]

# PDU (Power Distribution Unit) power metrics
PDU_POWER_METRICS = ['pdu']

# Metrics to exclude from graphs (not power consumption)
EXCLUDED_METRICS = [
    'systemheadroominstantaneous'  # This is remaining wattage, not consumption
]

# Derived metrics that are not real power consumption
DERIVED_METRICS = [
    'computepower',              # Compute power that is not wasted
    'systemheadroominstantaneous'  # This is remaining wattage, not consumption
]
