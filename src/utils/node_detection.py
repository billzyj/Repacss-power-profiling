"""
Node type detection utilities for REPACSS Power Measurement
"""

from typing import Tuple, Optional, List, Callable
from constants.nodes import IRC_NODES, PDU_NODES
from constants.metrics import IRC_POWER_METRICS, PDU_POWER_METRICS


def get_node_type_and_query_func(hostname: str) -> Tuple[str, Callable, str, str]:
    """
    Determine node type and appropriate query function based on hostname
    
    Args:
        hostname: Node hostname to analyze
    
    Returns:
        Tuple of (node_type, query_func, database, schema)
    """
    # Import query functions
    from queries.infra.irc_pdu import get_irc_metrics_with_joins, get_pdu_metrics_with_joins
    from queries.compute.idrac import get_compute_metrics_with_joins
    
    if hostname in IRC_NODES:
        return 'irc', get_irc_metrics_with_joins, 'infra', 'irc'
    elif hostname in PDU_NODES:
        return 'pdu', get_pdu_metrics_with_joins, 'infra', 'pdu'
    elif hostname.startswith('rpg-'):
        return 'compute', get_compute_metrics_with_joins, 'h100', 'idrac'
    elif hostname.startswith('rpc-'):
        return 'compute', get_compute_metrics_with_joins, 'zen4', 'idrac'
    else:
        # Default to compute node on h100 database
        return 'compute', get_compute_metrics_with_joins, 'h100', 'idrac'


def get_node_power_metrics(hostname: str) -> List[str]:
    """
    Get appropriate power metrics for a node type
    
    Args:
        hostname: Node hostname
    
    Returns:
        List of power metrics for the node type
    """
    node_type, _, _, _ = get_node_type_and_query_func(hostname)
    
    if node_type == 'irc':
        return IRC_POWER_METRICS
    elif node_type == 'pdu':
        return PDU_POWER_METRICS
    else:
        # For compute nodes, metrics are determined dynamically from database
        return []
