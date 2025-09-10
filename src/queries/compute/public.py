# Save some common queries for H100/Zen4

# All metrics
ALL_METRICS = """
SELECT 
    metric_id,
    metric_name,
    description,
    metric_data_type,
    units,
    accuracy,
    sensing_interval
FROM public.metrics_definition 
ORDER BY metric_name;
"""

# Power related metrics
POWER_METRICS_QUERY = """
SELECT 
    metric_id,
    metric_name,
    description,
    metric_data_type,
    units,
    accuracy,
    sensing_interval
FROM public.metrics_definition 
WHERE units IN ('mW', 'W', 'kW') OR metric_name LIKE '%Power%'
ORDER BY metric_name;
"""

# Get original power related metrics unit in mW, W, kW
POWER_METRICS_QUERY_UNIT_IN_MW_W_KW = """
SELECT 
    metric_id,
    metric_name,
    description,
    metric_data_type,
    units,
    accuracy,
    sensing_interval
FROM public.metrics_definition 
WHERE units IN ('mW', 'W', 'kW')
ORDER BY metric_name;
"""

# Get temperature-related metrics
TEMPERATURE_METRICS = """
SELECT 
    metric_id,
    metric_name,
    description,
    metric_data_type,
    units,
    accuracy,
    sensing_interval
FROM public.metrics_definition 
WHERE units IN ('C', 'F', 'K') OR metric_name LIKE '%Temperature%' OR metric_name LIKE '%Temp%'
ORDER BY metric_name;
"""