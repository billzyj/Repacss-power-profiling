# IRC and PDU Data Queries for INFRA Database

# ============================================================================
# SCHEMA INFORMATION QUERIES
# ============================================================================

# Get available PDU metrics
AVAILABLE_PDU_METRICS = """
SELECT 
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'pdu'
ORDER BY table_name, ordinal_position;
"""

# Get available IRC metrics
AVAILABLE_IRC_METRICS = """
SELECT 
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'irc'
ORDER BY table_name, ordinal_position;
"""

# Get infrastructure table statistics
INFRA_TABLE_STATS = """
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats
WHERE schemaname IN ('pdu', 'irc')
ORDER BY schemaname, tablename, attname;
"""

# ============================================================================
# GENERIC QUERY FUNCTIONS (HOSTNAME + UNITS)
# ============================================================================

def get_irc_metrics_with_hostname_and_units(table_name: str, limit: int = 100):
    """Get IRC metrics with hostname and units for any table"""
    return f"""
    SELECT 
        m.timestamp,
        n.hostname,
        m.value,
        md.units
    FROM irc.{table_name} m
    JOIN public.nodes n ON m.nodeid = n.nodeid
    LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('{table_name}')
    ORDER BY m.timestamp DESC
    LIMIT {limit};
    """

def get_pdu_metrics_with_hostname_and_units(table_name: str, limit: int = 100):
    """Get PDU metrics with hostname and units for any table"""
    return f"""
    SELECT 
        m.timestamp,
        n.hostname,
        m.value,
        md.units
    FROM pdu.{table_name} m
    JOIN public.nodes n ON m.nodeid = n.nodeid
    LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('{table_name}')
    ORDER BY m.timestamp DESC
    LIMIT {limit};
    """

def get_irc_summary_with_hostname_and_units(table_name: str, hours: int = 1):
    """Get IRC summary with hostname and units for any table"""
    return f"""
    SELECT 
        n.hostname,
        md.units,
        COUNT(*) as data_points,
        AVG(m.value) as avg_value,
        MIN(m.value) as min_value,
        MAX(m.value) as max_value,
        STDDEV(m.value) as value_stddev
    FROM irc.{table_name} m
    JOIN public.nodes n ON m.nodeid = n.nodeid
    LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('{table_name}')
    WHERE m.timestamp >= NOW() - INTERVAL '{hours} hour'
    GROUP BY n.hostname, md.units
    ORDER BY avg_value DESC;
    """

def get_pdu_summary_with_hostname_and_units(table_name: str, hours: int = 1):
    """Get PDU summary with hostname and units for any table"""
    return f"""
    SELECT 
        n.hostname,
        md.units,
        COUNT(*) as data_points,
        AVG(m.value) as avg_value,
        MIN(m.value) as min_value,
        MAX(m.value) as max_value,
        STDDEV(m.value) as value_stddev
    FROM pdu.{table_name} m
    JOIN public.nodes n ON m.nodeid = n.nodeid
    LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('{table_name}')
    WHERE m.timestamp >= NOW() - INTERVAL '{hours} hour'
    GROUP BY n.hostname, md.units
    ORDER BY avg_value DESC;
    """

def get_irc_metrics_with_hostname_and_units_by_time_range(table_name: str, start_time: str, end_time: str):
    """Get IRC metrics with hostname and units for a specific time range"""
    return f"""
    SELECT 
        m.timestamp,
        n.hostname,
        m.value,
        md.units
    FROM irc.{table_name} m
    JOIN public.nodes n ON m.nodeid = n.nodeid
    LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('{table_name}')
    WHERE m.timestamp BETWEEN '{start_time}' AND '{end_time}'
    ORDER BY m.timestamp DESC;
    """

def get_pdu_metrics_with_hostname_and_units_by_time_range(table_name: str, start_time: str, end_time: str):
    """Get PDU metrics with hostname and units for a specific time range"""
    return f"""
    SELECT 
        m.timestamp,
        n.hostname,
        m.value,
        md.units
    FROM pdu.{table_name} m
    JOIN public.nodes n ON m.nodeid = n.nodeid
    LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('{table_name}')
    WHERE m.timestamp BETWEEN '{start_time}' AND '{end_time}'
    ORDER BY m.timestamp DESC;
    """

# ============================================================================
# SPECIFIC IRC QUERIES WITH HOSTNAME AND UNITS
# ============================================================================

# Temperature-related metrics
IRC_ROOMTEMPERATURE_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.roomtemperature m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('roomtemperature')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

# Pressure-related metrics
IRC_SUCTIONPRESSURE_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.suctionpressure m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('suctionpressure')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

IRC_DISCHARGEPRESSURE_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.dischargepressure m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('dischargepressure')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

# Fan and motor control metrics
IRC_MODULATINGVALVEPOSITION_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.modulatingvalveposition m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('modulatingvalveposition')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

IRC_DRYCOOLERFANSPEED_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.drycoolerfanspeed m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('drycoolerfanspeed')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

IRC_EVAPORATORFANSPEED_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.evaporatorfanspeed m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('evaporatorfanspeed')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

IRC_CONDENSERFANSPEED_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.condenserfanspeed m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('condenserfanspeed')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

IRC_EEVPOSITION_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.eevposition m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('eevposition')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

# Power supply metrics
IRC_FANPOWERSUPPLY1_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.fanpowersupply1 m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('fanpowersupply1')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

IRC_FANPOWERSUPPLY2_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.fanpowersupply2 m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('fanpowersupply2')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

# Run hours metrics
IRC_FAN6RUNHOURS_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.fan6runhours m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('fan6runhours')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

# ============================================================================
# SPECIFIC PDU QUERIES WITH HOSTNAME AND UNITS
# ============================================================================

PDU_POWER_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM pdu.power m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('power')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

PDU_COMPRESSORPOWER_WITH_HOSTNAME_AND_UNITS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM pdu.compressorpower m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('compressorpower')
ORDER BY m.timestamp DESC
LIMIT %s;
"""

# ============================================================================
# ANALYSIS AND ALERT QUERIES
# ============================================================================

# Infrastructure efficiency analysis
INFRA_EFFICIENCY_ANALYSIS = """
SELECT 
    n.hostname,
    md_power.units as power_units,
    md_temp.units as temp_units,
    AVG(p.value) as avg_power,
    AVG(t.value) as avg_temperature,
    COUNT(p.value) as power_data_points,
    COUNT(t.value) as temp_data_points
FROM pdu.power p
JOIN public.nodes n ON p.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md_power ON LOWER(md_power.metric_id) = LOWER('power')
LEFT JOIN irc.roomtemperature t ON p.nodeid = t.nodeid AND p.timestamp = t.timestamp
LEFT JOIN public.metrics_definition md_temp ON LOWER(md_temp.metric_id) = LOWER('roomtemperature')
WHERE p.timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY n.hostname, md_power.units, md_temp.units
ORDER BY avg_power DESC;
"""

# High power usage alerts
INFRA_HIGH_POWER_ALERTS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM pdu.power m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('power')
WHERE m.value > %s
ORDER BY m.value DESC
LIMIT %s;
"""

# High temperature alerts
INFRA_HIGH_TEMPERATURE_ALERTS = """
SELECT 
    m.timestamp,
    n.hostname,
    m.value,
    md.units
FROM irc.roomtemperature m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('roomtemperature')
WHERE m.value > %s
ORDER BY m.value DESC
LIMIT %s;
"""

# Infrastructure cluster summary
INFRA_CLUSTER_SUMMARY = """
SELECT 
    COUNT(DISTINCT n.hostname) as total_nodes,
    COUNT(*) as total_data_points,
    AVG(m.value) as cluster_avg_value,
    MIN(m.value) as cluster_min_value,
    MAX(m.value) as cluster_max_value,
    STDDEV(m.value) as cluster_stddev,
    md.units
FROM pdu.power m
JOIN public.nodes n ON m.nodeid = n.nodeid
LEFT JOIN public.metrics_definition md ON LOWER(md.metric_id) = LOWER('power')
WHERE m.timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY md.units;
"""
