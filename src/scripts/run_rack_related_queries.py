# This script is used to run rack-related power queries and validate power consumption
# It compares compute node power consumption with PDU power consumption for validation

import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.power_utils import multi_node_power_analysis, RACK_91_COMPUTE_NODES, RACK_91_PD_NODES, RACK_97_COMPUTE_NODES, RACK_97_PDU_NODES
from core.power_utils import get_compute_power_metrics, PDU_POWER_METRICS

def main():
    """Main function to run rack-related power queries and validation"""
    print("🏗️ Rack Power Analysis and Validation Runner")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print()
    
    # Get the last 24 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    print(f"📊 Analyzing power data from {start_time} to {end_time}")
    print()
    
    # Analyze Rack 97
    print("🔧 Analyzing Rack 97...")
    print("  📊 Computing nodes system input power...")
    rack97_compute_results = multi_node_power_analysis(RACK_97_COMPUTE_NODES, start_time, end_time, ['systeminputpower'])
    
    print("  ⚡ PDU nodes power consumption...")
    rack97_pdu_results = multi_node_power_analysis(RACK_97_PDU_NODES, start_time, end_time, PDU_POWER_METRICS)
    
    # Analyze Rack 91
    print("🔧 Analyzing Rack 91...")
    print("  📊 Computing nodes system input power...")
    rack91_compute_results = multi_node_power_analysis(RACK_91_COMPUTE_NODES, start_time, end_time, ['systeminputpower'])
    
    print("  ⚡ PDU nodes power consumption...")
    rack91_pdu_results = multi_node_power_analysis(RACK_91_PD_NODES, start_time, end_time, PDU_POWER_METRICS)
    
    # Save Rack 97 results
    if rack97_compute_results or rack97_pdu_results:
        print("💾 Saving Rack 97 results...")
        save_rack_analysis("Rack97", rack97_compute_results, rack97_pdu_results, start_time)
    
    # Save Rack 91 results
    if rack91_compute_results or rack91_pdu_results:
        print("💾 Saving Rack 91 results...")
        save_rack_analysis("Rack91", rack91_compute_results, rack91_pdu_results, start_time)
    
    print()
    print("🎉 Rack analysis complete!")
    print(f"📅 Finished at: {datetime.now()}")

def save_rack_analysis(rack_name, compute_results, pdu_results, start_time):
    """Save rack analysis results to Excel with validation summary"""
    
    # Create output directory
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'output')
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
        
        # Calculate difference and percentage
        if total_pdu_energy > 0:
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
    print(f"   ⚡ Total PDU energy: {total_pdu_energy:.2f} kWh")
    if total_pdu_energy > 0:
        diff_percent = ((total_compute_energy - total_pdu_energy) / total_pdu_energy) * 100
        print(f"   🔍 Energy difference: {diff_percent:.2f}%")
        if abs(diff_percent) < 10:
            print(f"   ✅ Power validation: GOOD (within 10% tolerance)")
        elif abs(diff_percent) < 20:
            print(f"   ⚠️ Power validation: ACCEPTABLE (within 20% tolerance)")
        else:
            print(f"   ❌ Power validation: NEEDS INVESTIGATION (>20% difference)")

if __name__ == "__main__":
    main()
