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

# Get sources by category
SOURCES_BY_CATEGORY = """
SELECT 
    CASE 
        WHEN source LIKE '%Power%' OR source LIKE '%psu%' THEN 'Power'
        WHEN source LIKE '%thermal%' OR source LIKE '%temp%' OR source LIKE '%fan%' THEN 'Thermal'
        WHEN source LIKE '%Memory%' OR source LIKE '%memory%' THEN 'Memory'
        WHEN source LIKE '%CPU%' OR source LIKE '%cpu%' THEN 'CPU'
        WHEN source LIKE '%storage%' OR source LIKE '%smart%' OR source LIKE '%nvme%' THEN 'Storage'
        WHEN source LIKE '%nic%' OR source LIKE '%sfp%' OR source LIKE '%transceiver%' THEN 'Network'
        ELSE 'Other'
    END as category,
    COUNT(*) as source_count,
    STRING_AGG(source, ', ' ORDER BY source) as sources
FROM public.source
GROUP BY 
    CASE 
        WHEN source LIKE '%Power%' OR source LIKE '%psu%' THEN 'Power'
        WHEN source LIKE '%thermal%' OR source LIKE '%temp%' OR source LIKE '%fan%' THEN 'Thermal'
        WHEN source LIKE '%Memory%' OR source LIKE '%memory%' THEN 'Memory'
        WHEN source LIKE '%CPU%' OR source LIKE '%cpu%' THEN 'CPU'
        WHEN source LIKE '%storage%' OR source LIKE '%smart%' OR source LIKE '%nvme%' THEN 'Storage'
        WHEN source LIKE '%nic%' OR source LIKE '%sfp%' OR source LIKE '%transceiver%' THEN 'Network'
        ELSE 'Other'
    END
ORDER BY category;
"""