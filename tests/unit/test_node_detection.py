"""
Unit tests for node detection utilities
"""
import pytest
from src.utils.node_detection import get_node_type_and_query_func, get_node_power_metrics


class TestNodeDetection:
    """Test node type detection and classification"""
    
    def test_h100_node_detection(self):
        """Test detection of H100 compute nodes"""
        node_type, query_func, database, schema = get_node_type_and_query_func('rpg-93-1')
        
        assert node_type == 'compute'
        assert database == 'h100'
        assert schema == 'idrac'
        assert callable(query_func)
    
    def test_zen4_node_detection(self):
        """Test detection of ZEN4 compute nodes"""
        node_type, query_func, database, schema = get_node_type_and_query_func('rpc-91-1')
        
        assert node_type == 'compute'
        assert database == 'zen4'
        assert schema == 'idrac'
        assert callable(query_func)
    
    def test_irc_node_detection(self):
        """Test detection of IRC infrastructure nodes"""
        # Assuming IRC_NODES contains 'irc-node-1'
        node_type, query_func, database, schema = get_node_type_and_query_func('irc-node-1')
        
        assert node_type == 'irc'
        assert database == 'infra'
        assert schema == 'irc'
        assert callable(query_func)
    
    def test_pdu_node_detection(self):
        """Test detection of PDU infrastructure nodes"""
        # Assuming PDU_NODES contains 'pdu-node-1'
        node_type, query_func, database, schema = get_node_type_and_query_func('pdu-node-1')
        
        assert node_type == 'pdu'
        assert database == 'infra'
        assert schema == 'pdu'
        assert callable(query_func)
    
    def test_unknown_node_defaults_to_h100(self):
        """Test that unknown nodes default to H100 compute"""
        node_type, query_func, database, schema = get_node_type_and_query_func('unknown-node')
        
        assert node_type == 'compute'
        assert database == 'h100'
        assert schema == 'idrac'
        assert callable(query_func)
    
    def test_get_node_power_metrics(self):
        """Test getting power metrics for different node types"""
        # Test compute node metrics
        metrics = get_node_power_metrics('rpg-93-1')
        assert isinstance(metrics, list)
        assert len(metrics) > 0
        
        # Test IRC node metrics
        metrics = get_node_power_metrics('irc-node-1')
        assert isinstance(metrics, list)
        assert len(metrics) > 0
