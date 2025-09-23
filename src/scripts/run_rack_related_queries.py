# This script is used to run rack-related power queries and validate power consumption
# It compares compute node power consumption with PDU power consumption for validation
# Handles all racks (91-97) with different analysis approaches based on rack configuration

import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.power_utils import multi_node_power_analysis, PDU_POWER_METRICS
from core.power_utils import (
    RACK_91_COMPUTE_NODES, RACK_91_PD_NODES,
    RACK_92_COMPUTE_NODES, RACK_92_PD_NODES,
    RACK_93_COMPUTE_NODES, RACK_93_PD_NODES,
    RACK_94_COMPUTE_NODES, RACK_94_PD_NODES,
    RACK_95_COMPUTE_NODES, RACK_95_PD_NODES,
    RACK_96_COMPUTE_NODES, RACK_96_PD_NODES,
    RACK_97_COMPUTE_NODES, RACK_97_PDU_NODES
)

# Rack configuration definitions
RACK_CONFIGS = {
    91: {
        'compute_nodes': RACK_91_COMPUTE_NODES,
        'pdu_nodes': RACK_91_PD_NODES,
        'analysis_type': 'estimated_switches',
        'description': 'Has 1 ethernet switch + 1 infiniband switch (unmeasured)',
        'estimated_switch_power_kw': 2.0  # Estimated power for 2 switches
    },
    92: {
        'compute_nodes': RACK_92_COMPUTE_NODES,
        'pdu_nodes': RACK_92_PD_NODES,
        'analysis_type': 'estimated_amd_nodes',
        'description': 'Has 2 AMD test nodes (unmeasured)',
        'estimated_switch_power_kw': 1.0  # Estimated power for AMD nodes
    },
    93: {
        'compute_nodes': RACK_93_COMPUTE_NODES,
        'pdu_nodes': RACK_93_PD_NODES,
        'analysis_type': 'estimated_mixed',
        'description': 'Has ethernet switch, TTU switch, 1GB switch, GPU-build node, login node, 2 head nodes, monitor node, globus node',
        'estimated_switch_power_kw': 3.0  # Estimated power for multiple components
    },
    94: {
        'compute_nodes': RACK_94_COMPUTE_NODES,
        'pdu_nodes': RACK_94_PD_NODES,
        'analysis_type': 'estimated_switches',
        'description': 'Has 1 ethernet switch + 1 infiniband switch (unmeasured)',
        'estimated_switch_power_kw': 2.0  # Estimated power for 2 switches
    },
    95: {
        'compute_nodes': RACK_95_COMPUTE_NODES,
        'pdu_nodes': RACK_95_PD_NODES,
        'analysis_type': 'estimated_switches',
        'description': 'Has 1 ethernet switch + 2 infiniband switches + 9 hammerspace nodes (unmeasured)',
        'estimated_switch_power_kw': 4.0  # Estimated power for switches and hammerspace nodes
    },
    96: {
        'compute_nodes': RACK_96_COMPUTE_NODES,
        'pdu_nodes': RACK_96_PD_NODES,
        'analysis_type': 'estimated_switches',
        'description': 'Has 1 ethernet switch + 1 infiniband switch (unmeasured)',
        'estimated_switch_power_kw': 2.0  # Estimated power for 2 switches
    },
    97: {
        'compute_nodes': RACK_97_COMPUTE_NODES,
        'pdu_nodes': RACK_97_PDU_NODES,
        'analysis_type': 'accurate',
        'description': 'All compute nodes connected to all PDUs - most accurate analysis',
        'estimated_switch_power_kw': 0.0  # No additional unmeasured components
    }
}

def main():
    """Main function to run rack-related power queries and validation for all racks"""
    print("🏗️ Comprehensive Rack Power Analysis and Validation Runner")
    print("=" * 70)
    print(f"📅 Started at: {datetime.now()}")
    print()
    
    # Set specific date range for analysis
    start_time = datetime(2025, 9, 12, 0, 0, 0)
    end_time = datetime(2025, 9, 14, 0, 0, 0)
    
    print(f"📊 Analyzing power data from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Analyze all racks
    for rack_num in sorted(RACK_CONFIGS.keys()):
        config = RACK_CONFIGS[rack_num]
        print(f"🔧 Analyzing Rack {rack_num} ({config['analysis_type']})...")
        print(f"  📝 {config['description']}")
        
        # Get compute node power
        print("  📊 Computing nodes system input power...")
        compute_results = multi_node_power_analysis(
            config['compute_nodes'], 
            start_time, 
            end_time, 
            ['systeminputpower']
        )
        
        # Get PDU power
        print("  ⚡ PDU nodes power consumption...")
        pdu_results = multi_node_power_analysis(
            config['pdu_nodes'], 
            start_time, 
            end_time, 
            PDU_POWER_METRICS
        )
        
        # Save results with appropriate analysis type
        if compute_results or pdu_results:
            print(f"  💾 Saving Rack {rack_num} results...")
            save_rack_analysis(
                f"Rack{rack_num}", 
                compute_results, 
                pdu_results, 
                start_time,
                end_time,
                config['analysis_type'],
                config['estimated_switch_power_kw']
            )
        
        print()
    
    print("🎉 All rack analysis complete!")
    print_summary()
    print(f"📅 Finished at: {datetime.now()}")

def print_summary():
    """Print a summary of all rack configurations and analysis types"""
    print()
    print("📋 Rack Analysis Summary:")
    print("=" * 50)
    
    for rack_num in sorted(RACK_CONFIGS.keys()):
        config = RACK_CONFIGS[rack_num]
        print(f"🔧 Rack {rack_num}: {config['analysis_type']}")
        print(f"   📝 {config['description']}")
        print(f"   ⚡ Estimated additional power: {config['estimated_switch_power_kw']} kW")
        print()
    
    print("📊 Analysis Types:")
    print("   ✅ accurate: Direct comparison (Rack 97 only)")
    print("   📊 estimated_switches: Includes estimated switch power")
    print("   📊 estimated_amd_nodes: Includes estimated AMD node power")
    print("   📊 estimated_mixed: Includes estimated mixed component power")
    print()

def save_rack_analysis(rack_name, compute_results, pdu_results, start_time, end_time, analysis_type='accurate', estimated_switch_power_kw=0.0):
    """Save rack analysis results to Excel with validation summary"""
    
    # Create output directory under output/rack
    base_output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'output')
    output_dir = os.path.join(base_output_dir, 'rack')
    os.makedirs(output_dir, exist_ok=True)
    
    filename = os.path.join(output_dir, f"{rack_name.lower()}_power_analysis_{start_time.strftime('%Y%m%d_%H%M%S')}.xlsx")
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Process compute nodes
        compute_combined_data = []
        compute_energy_summary = {}
        total_compute_energy = 0
        
        if compute_results:
            for hostname, (df, energy_dict) in compute_results.items():
                if not df.empty:
                    compute_combined_data.append(df)
                    compute_energy_summary[hostname] = energy_dict
                    # Sum up all energy for this compute node
                    for metric, energy_kwh in energy_dict.items():
                        total_compute_energy += energy_kwh
        
        # Process PDU nodes
        pdu_combined_data = []
        pdu_energy_summary = {}
        total_pdu_energy = 0
        
        if pdu_results:
            for hostname, (df, energy_dict) in pdu_results.items():
                if not df.empty:
                    pdu_combined_data.append(df)
                    pdu_energy_summary[hostname] = energy_dict
                    # Sum up all energy for this PDU node
                    for metric, energy_kwh in energy_dict.items():
                        total_pdu_energy += energy_kwh
        
        # Save compute nodes data
        if compute_combined_data:
            compute_combined_df = pd.concat(compute_combined_data, ignore_index=True)
            
            # Convert timezone-aware timestamps to timezone-naive for Excel compatibility
            if 'timestamp' in compute_combined_df.columns:
                compute_combined_df['timestamp'] = compute_combined_df['timestamp'].dt.tz_localize(None)
            
            compute_combined_df.to_excel(writer, sheet_name='Compute_Nodes', index=False)
        
        # Save PDU nodes data
        if pdu_combined_data:
            pdu_combined_df = pd.concat(pdu_combined_data, ignore_index=True)
            
            # Convert timezone-aware timestamps to timezone-naive for Excel compatibility
            if 'timestamp' in pdu_combined_df.columns:
                pdu_combined_df['timestamp'] = pdu_combined_df['timestamp'].dt.tz_localize(None)
            
            pdu_combined_df.to_excel(writer, sheet_name='PDU_Nodes', index=False)
        
        # Create validation summary
        validation_data = []
        
        # Add compute nodes energy summary
        for hostname, energy_dict in compute_energy_summary.items():
            for metric, energy_kwh in energy_dict.items():
                validation_data.append({
                    'node_type': 'Compute',
                    'hostname': hostname,
                    'metric': metric,
                    'total_energy_kwh': energy_kwh
                })
        
        # Add PDU nodes energy summary
        for hostname, energy_dict in pdu_energy_summary.items():
            for metric, energy_kwh in energy_dict.items():
                validation_data.append({
                    'node_type': 'PDU',
                    'hostname': hostname,
                    'metric': metric,
                    'total_energy_kwh': energy_kwh
                })
        
        # Add totals
        validation_data.append({
            'node_type': 'TOTAL',
            'hostname': f'{rack_name} Compute Total (SystemInputPower)',
            'metric': 'systeminputpower',
            'total_energy_kwh': total_compute_energy
        })
        
        validation_data.append({
            'node_type': 'TOTAL',
            'hostname': f'{rack_name} PDU Total',
            'metric': 'pdu',
            'total_energy_kwh': total_pdu_energy
        })
        
        # Calculate estimated switch energy for the time period
        time_hours = (end_time - start_time).total_seconds() / 3600  # Calculate actual hours
        estimated_switch_energy = estimated_switch_power_kw * time_hours
        
        # Add estimated switch energy to validation
        if estimated_switch_energy > 0:
            validation_data.append({
                'node_type': 'ESTIMATED',
                'hostname': f'{rack_name} Estimated Switch/Unmeasured Components',
                'metric': 'estimated',
                'total_energy_kwh': estimated_switch_energy
            })
        
        # Calculate adjusted totals for validation
        adjusted_compute_energy = total_compute_energy + estimated_switch_energy
        
        # Calculate difference and percentage
        if total_pdu_energy > 0:
            # For accurate analysis (Rack 97), compare directly
            if analysis_type == 'accurate':
                energy_difference = total_compute_energy - total_pdu_energy
                energy_percentage_diff = (energy_difference / total_pdu_energy) * 100
                
                validation_data.append({
                    'node_type': 'VALIDATION',
                    'hostname': 'Energy Difference (SystemInputPower - PDU)',
                    'metric': 'kWh',
                    'total_energy_kwh': energy_difference
                })
                
                validation_data.append({
                    'node_type': 'VALIDATION',
                    'hostname': 'Percentage Difference',
                    'metric': '%',
                    'total_energy_kwh': energy_percentage_diff
                })
            else:
                # For estimated analysis, compare adjusted compute energy
                adjusted_energy_difference = adjusted_compute_energy - total_pdu_energy
                adjusted_energy_percentage_diff = (adjusted_energy_difference / total_pdu_energy) * 100
                
                validation_data.append({
                    'node_type': 'VALIDATION',
                    'hostname': 'Adjusted Energy Difference (SystemInputPower + Estimated - PDU)',
                    'metric': 'kWh',
                    'total_energy_kwh': adjusted_energy_difference
                })
                
                validation_data.append({
                    'node_type': 'VALIDATION',
                    'hostname': 'Adjusted Percentage Difference',
                    'metric': '%',
                    'total_energy_kwh': adjusted_energy_percentage_diff
                })
                
                # Also show raw difference for reference
                raw_energy_difference = total_compute_energy - total_pdu_energy
                raw_energy_percentage_diff = (raw_energy_difference / total_pdu_energy) * 100
                
                validation_data.append({
                    'node_type': 'VALIDATION',
                    'hostname': 'Raw Energy Difference (SystemInputPower - PDU)',
                    'metric': 'kWh',
                    'total_energy_kwh': raw_energy_difference
                })
                
                validation_data.append({
                    'node_type': 'VALIDATION',
                    'hostname': 'Raw Percentage Difference',
                    'metric': '%',
                    'total_energy_kwh': raw_energy_percentage_diff
                })
        
        # Save validation summary
        if validation_data:
            validation_df = pd.DataFrame(validation_data)
            validation_df.to_excel(writer, sheet_name='Validation_Summary', index=False)
        
        # Create power comparison chart data
        comparison_data = []
        if total_compute_energy > 0 and total_pdu_energy > 0:
            comparison_data.append({
                'Power_Source': 'SystemInputPower (Compute Nodes)',
                'Total_Energy_kWh': total_compute_energy,
                'Percentage_of_PDU': (total_compute_energy / total_pdu_energy) * 100
            })
            
            if estimated_switch_energy > 0:
                comparison_data.append({
                    'Power_Source': 'Estimated Switch/Unmeasured Components',
                    'Total_Energy_kWh': estimated_switch_energy,
                    'Percentage_of_PDU': (estimated_switch_energy / total_pdu_energy) * 100
                })
                
                comparison_data.append({
                    'Power_Source': 'Total Compute + Estimated',
                    'Total_Energy_kWh': adjusted_compute_energy,
                    'Percentage_of_PDU': (adjusted_compute_energy / total_pdu_energy) * 100
                })
            
            comparison_data.append({
                'Power_Source': 'PDU Nodes',
                'Total_Energy_kWh': total_pdu_energy,
                'Percentage_of_PDU': 100.0
            })
        
        if comparison_data:
            comparison_df = pd.DataFrame(comparison_data)
            comparison_df.to_excel(writer, sheet_name='Power_Comparison', index=False)
    
    print(f"✅ {rack_name} results saved to: {filename}")
    print(f"   📈 Total SystemInputPower energy: {total_compute_energy:.2f} kWh")
    if estimated_switch_energy > 0:
        print(f"   🔌 Estimated switch/unmeasured energy: {estimated_switch_energy:.2f} kWh")
        print(f"   📊 Total adjusted compute energy: {adjusted_compute_energy:.2f} kWh")
    print(f"   ⚡ Total PDU energy: {total_pdu_energy:.2f} kWh")
    
    if total_pdu_energy > 0:
        if analysis_type == 'accurate':
            # For accurate analysis (Rack 97), use direct comparison
            diff_percent = ((total_compute_energy - total_pdu_energy) / total_pdu_energy) * 100
            print(f"   🔍 Energy difference: {diff_percent:.2f}%")
            if abs(diff_percent) < 10:
                print(f"   ✅ Power validation: GOOD (within 10% tolerance)")
            elif abs(diff_percent) < 20:
                print(f"   ⚠️ Power validation: ACCEPTABLE (within 20% tolerance)")
            else:
                print(f"   ❌ Power validation: NEEDS INVESTIGATION (>20% difference)")
        else:
            # For estimated analysis, show both raw and adjusted differences
            raw_diff_percent = ((total_compute_energy - total_pdu_energy) / total_pdu_energy) * 100
            adjusted_diff_percent = ((adjusted_compute_energy - total_pdu_energy) / total_pdu_energy) * 100
            
            print(f"   🔍 Raw energy difference: {raw_diff_percent:.2f}%")
            print(f"   🔍 Adjusted energy difference: {adjusted_diff_percent:.2f}%")
            
            # Use adjusted difference for validation
            if abs(adjusted_diff_percent) < 10:
                print(f"   ✅ Power validation: GOOD (within 10% tolerance)")
            elif abs(adjusted_diff_percent) < 20:
                print(f"   ⚠️ Power validation: ACCEPTABLE (within 20% tolerance)")
            else:
                print(f"   ❌ Power validation: NEEDS INVESTIGATION (>20% difference)")
            
            print(f"   📝 Note: Analysis includes estimated power for unmeasured components")

if __name__ == "__main__":
    main()
