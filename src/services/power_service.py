#!/usr/bin/env python3
"""
Power analysis service layer
Handles high-level power analysis operations without direct database coupling
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.power import PowerAnalyzer
from analysis.energy import EnergyCalculator
from queries.manager import QueryManager
from database.connection_pool import close_all_pools


class PowerAnalysisService:
    """High-level service for power analysis operations"""
    
    def __init__(self, database: str):
        self.database = database
        self.query_manager = QueryManager(database)
        self.power_analyzer = PowerAnalyzer(database)
        self.energy_calculator = EnergyCalculator(database)
    
    def get_system_overview(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get system-wide power overview for the specified database.
        
        Args:
            hours: Number of hours to analyze
        
        Returns:
            System overview with key metrics
        """
        end_time = datetime.now()
        start_time = end_time - pd.Timedelta(hours=hours)
        
        try:
            # Get available nodes
            nodes = self._get_available_nodes()
            
            # Get system metrics
            system_metrics = self._get_system_metrics(start_time, end_time)
            
            return {
                'database': self.database,
                'time_range': {
                    'start': start_time,
                    'end': end_time,
                    'hours': hours
                },
                'nodes': nodes,
                'metrics': system_metrics,
                'summary': self._calculate_system_summary(system_metrics)
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_node_power(self, hostname: str, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Analyze power consumption for a specific node.
        
        Args:
            hostname: Node hostname
            start_time: Start timestamp
            end_time: End timestamp
        
        Returns:
            Node power analysis results
        """
        try:
            # Run power analysis
            power_results = self.power_analyzer.analyze_power(hostname, start_time, end_time)
            
            # Calculate energy consumption
            energy_results = self.energy_calculator.calculate_energy(hostname, start_time, end_time)
            
            return {
                'hostname': hostname,
                'time_range': {
                    'start': start_time,
                    'end': end_time
                },
                'power_analysis': power_results,
                'energy_consumption': energy_results,
                'summary': self._create_node_summary(power_results, energy_results)
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_rack_power(self, rack_number: int, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Analyze power consumption for an entire rack.
        
        Args:
            rack_number: Rack number (91-97)
            start_time: Start timestamp
            end_time: End timestamp
        
        Returns:
            Rack power analysis results
        """
        try:
            # Get rack nodes
            rack_nodes = self._get_rack_nodes(rack_number)
            
            # Analyze each node
            node_results = {}
            total_energy = 0.0
            
            for node in rack_nodes:
                node_analysis = self.analyze_node_power(node, start_time, end_time)
                if 'error' not in node_analysis:
                    node_results[node] = node_analysis
                    if 'energy_consumption' in node_analysis:
                        total_energy += sum(node_analysis['energy_consumption'].values())
            
            return {
                'rack_number': rack_number,
                'time_range': {
                    'start': start_time,
                    'end': end_time
                },
                'nodes': list(rack_nodes),
                'node_analyses': node_results,
                'total_energy_kwh': total_energy,
                'summary': self._create_rack_summary(node_results, total_energy)
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def get_available_metrics(self) -> List[str]:
        """Get list of available power metrics for the database"""
        try:
            metrics_df = self.query_manager.get_power_metrics_definition()
            return metrics_df['metric_id'].tolist() if not metrics_df.empty else []
        except Exception as e:
            return []
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information and statistics"""
        try:
            return self.query_manager.get_database_info()
        except Exception as e:
            return {'error': str(e)}
    
    def _get_available_nodes(self) -> List[str]:
        """Get list of available nodes in the database"""
        # This would query the database for available nodes
        # For now, return empty list - would need to implement based on database schema
        return []
    
    def _get_system_metrics(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get system-wide metrics"""
        try:
            # Get power metrics for all nodes
            all_metrics = self.query_manager.get_power_metrics(
                hostname=None, start_time=start_time, end_time=end_time, limit=1000
            )
            
            return {
                'total_records': len(all_metrics),
                'metrics_available': len(all_metrics['metric'].unique()) if not all_metrics.empty else 0,
                'time_range_hours': (end_time - start_time).total_seconds() / 3600
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def _calculate_system_summary(self, system_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate system summary statistics"""
        if 'error' in system_metrics:
            return system_metrics
        
        return {
            'status': 'healthy' if system_metrics.get('total_records', 0) > 0 else 'no_data',
            'total_records': system_metrics.get('total_records', 0),
            'metrics_count': system_metrics.get('metrics_available', 0),
            'time_range_hours': system_metrics.get('time_range_hours', 0)
        }
    
    def _create_node_summary(self, power_results: Dict[str, Any], energy_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create summary for node analysis"""
        summary = {
            'status': 'success' if power_results else 'no_data',
            'has_power_data': bool(power_results),
            'has_energy_data': bool(energy_results)
        }
        
        if power_results and 'summary' in power_results:
            power_summary = power_results['summary']
            summary.update({
                'total_records': power_summary.get('total_records', 0),
                'avg_power_w': power_summary.get('avg_power_w', 0),
                'max_power_w': power_summary.get('max_power_w', 0),
                'total_energy_kwh': power_summary.get('total_energy_kwh', 0)
            })
        
        if energy_results:
            summary['total_energy_kwh'] = sum(energy_results.values())
        
        return summary
    
    def _create_rack_summary(self, node_results: Dict[str, Any], total_energy: float) -> Dict[str, Any]:
        """Create summary for rack analysis"""
        return {
            'status': 'success' if node_results else 'no_data',
            'nodes_analyzed': len(node_results),
            'total_energy_kwh': total_energy,
            'avg_energy_per_node': total_energy / len(node_results) if node_results else 0
        }
    
    def _get_rack_nodes(self, rack_number: int) -> List[str]:
        """Get nodes for a specific rack"""
        # Import rack configurations
        from constants.nodes import (
            RACK_91_COMPUTE_NODES, RACK_91_PD_NODES,
            RACK_92_COMPUTE_NODES, RACK_92_PD_NODES,
            RACK_93_COMPUTE_NODES, RACK_93_PD_NODES,
            RACK_94_COMPUTE_NODES, RACK_94_PD_NODES,
            RACK_95_COMPUTE_NODES, RACK_95_PD_NODES,
            RACK_96_COMPUTE_NODES, RACK_96_PD_NODES,
            RACK_97_COMPUTE_NODES, RACK_97_PDU_NODES
        )
        
        rack_configs = {
            91: RACK_91_COMPUTE_NODES + RACK_91_PD_NODES,
            92: RACK_92_COMPUTE_NODES + RACK_92_PD_NODES,
            93: RACK_93_COMPUTE_NODES + RACK_93_PD_NODES,
            94: RACK_94_COMPUTE_NODES + RACK_94_PD_NODES,
            95: RACK_95_COMPUTE_NODES + RACK_95_PD_NODES,
            96: RACK_96_COMPUTE_NODES + RACK_96_PD_NODES,
            97: RACK_97_COMPUTE_NODES + RACK_97_PDU_NODES
        }
        
        return rack_configs.get(rack_number, [])
    
    def __del__(self):
        """Cleanup connections when service is destroyed"""
        try:
            close_all_pools()
        except:
            pass
