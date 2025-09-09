# IRC Metrics Definition Queries for INFRA Database

# Get all IRC infrastructure metrics with their definitions
ALL_INFRA_METRICS = """
SELECT 
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
ORDER BY metric_id;
"""

# Get IRC power-related metrics from infra database
INFRA_POWER_METRICS = """
SELECT 
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
WHERE units IN ('kW', 'W', 'mW') OR metric_id LIKE '%Power%'
ORDER BY metric_id;
"""

# Get IRC temperature-related metrics
TEMPERATURE_METRICS = """
SELECT 
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
WHERE metric_id LIKE '%Temperature%' OR units IN ('C', 'F', 'K')
ORDER BY metric_id;
"""

# Get IRC compressor-related metrics
COMPRESSOR_METRICS = """
SELECT 
    id,
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
WHERE metric_id LIKE '%Compressor%'
ORDER BY metric_id;
"""

# Get IRC air filter and airflow metrics
AIR_SYSTEM_METRICS = """
SELECT 
    id,
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
WHERE metric_id LIKE '%Air%' OR metric_id LIKE '%Filter%' OR metric_id LIKE '%Flow%'
ORDER BY metric_id;
"""

# Get IRC run hours metrics
RUN_HOURS_METRICS = """
SELECT 
    id,
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
WHERE metric_id LIKE '%Run%' OR metric_id LIKE '%Hours%'
ORDER BY metric_id;
"""

# Get IRC humidity-related metrics
HUMIDITY_METRICS = """
SELECT 
    id,
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
WHERE metric_id LIKE '%Humidity%' OR metric_id LIKE '%RH%'
ORDER BY metric_id;
"""

# Get IRC pressure-related metrics
PRESSURE_METRICS = """
SELECT 
    id,
    metric_id,
    metric_data_type,
    units
FROM public.metrics_definition 
WHERE metric_id LIKE '%Pressure%' OR units IN ('Pa', 'kPa', 'bar', 'psi')
ORDER BY metric_id;
"""


# Get nodes with their available IRC metrics
NODES_WITH_METRICS = """
SELECT 
    n.nodeid,
    n.hostname,
    n.ip_addr,
    COUNT(md.metric_id) as available_metrics,
    STRING_AGG(md.metric_id, ', ' ORDER BY md.metric_id) as metrics
FROM public.nodes n
LEFT JOIN public.metrics_definition md ON 1=1  -- Cross join to get all IRC metrics
GROUP BY n.nodeid, n.hostname, n.ip_addr
ORDER BY n.nodeid;
"""
