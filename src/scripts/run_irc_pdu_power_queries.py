# This script is used to run the irc and pdu queries, and save the results to Excel files

import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.power_utils import multi_node_power_analysis, IRC_NODES, PDU_NODES

# Specifically, we want to run the irc and pdu queries for the last 24 hours, 
# calculate the energy consumption for each metric for all irc nodes and pdu nodes,
# and save the results to Excel files

def main():
    """Main function to run the irc and pdu queries"""
    print("🚀 IRC and PDU Power Analysis Runner")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print()
    
    # Use specific time period (08/15/2025 00:00:00 to 09/01/2025 00:00:00)
    start_time = datetime(2025, 8, 15, 0, 0, 0)
    end_time = datetime(2025, 8, 21, 0, 0, 0)
    
    print(f"📊 Analyzing power data from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Analyze IRC nodes
    print("🔧 Analyzing IRC nodes...")
    irc_results = multi_node_power_analysis(IRC_NODES, start_time, end_time)
    
    # Analyze PDU nodes  
    print("⚡ Analyzing PDU nodes...")
    pdu_results = multi_node_power_analysis(PDU_NODES, start_time, end_time)
    
    # Combine and save IRC results
    if irc_results:
        print("💾 Saving IRC results...")
        irc_combined_data = []
        irc_energy_summary = {}
        
        for hostname, (df, energy_dict) in irc_results.items():
            if not df.empty:
                irc_combined_data.append(df)
                irc_energy_summary[hostname] = energy_dict
        
        if irc_combined_data:
            irc_combined_df = pd.concat(irc_combined_data, ignore_index=True)
            
            # Convert timezone-aware timestamps to timezone-naive for Excel compatibility
            if 'timestamp' in irc_combined_df.columns:
                irc_combined_df['timestamp'] = irc_combined_df['timestamp'].dt.tz_localize(None)
            
            # Save to Excel in output/irc_pdu folder
            output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'output', 'irc_pdu')
            os.makedirs(output_dir, exist_ok=True)
            irc_filename = os.path.join(output_dir, f"irc_power_analysis_{start_time.strftime('%Y%m%d_%H%M%S')}.xlsx")
            with pd.ExcelWriter(irc_filename, engine='openpyxl') as writer:
                # Create individual tabs for each metric
                metrics = irc_combined_df['metric'].unique() if 'metric' in irc_combined_df.columns else []
                for metric in sorted(metrics):
                    metric_df = irc_combined_df[irc_combined_df['metric'] == metric].copy()
                    # Clean sheet name (Excel sheet names have restrictions)
                    sheet_name = metric.replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_').replace('[', '_').replace(']', '_').replace(':', '_')
                    sheet_name = sheet_name[:31]  # Excel sheet name limit
                    metric_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Create energy summary sheet with metric totals
                energy_summary_data = []
                metric_totals = {}
                
                # Collect all data and calculate metric totals
                for hostname, energy_dict in irc_energy_summary.items():
                    for metric, energy_kwh in energy_dict.items():
                        energy_summary_data.append({
                            'hostname': hostname,
                            'metric': metric,
                            'total_energy_kwh': energy_kwh
                        })
                        # Track metric totals
                        if metric not in metric_totals:
                            metric_totals[metric] = 0
                        metric_totals[metric] += energy_kwh
                
                # Add metric total rows
                for metric, total_energy in metric_totals.items():
                    energy_summary_data.append({
                        'hostname': f'**{metric} TOTAL**',
                        'metric': metric,
                        'total_energy_kwh': total_energy
                    })
                
                if energy_summary_data:
                    energy_summary_df = pd.DataFrame(energy_summary_data)
                    # Sort by metric, then by hostname (with totals at the end of each metric group)
                    energy_summary_df = energy_summary_df.sort_values(['metric', 'hostname'])
                    energy_summary_df.to_excel(writer, sheet_name='Energy_Summary', index=False)
            
            print(f"✅ IRC results saved to: {irc_filename}")
            print(f"   📈 Total data points: {len(irc_combined_df)}")
            print(f"   🔋 Total energy metrics: {len(energy_summary_data)}")
    
    # Combine and save PDU results
    if pdu_results:
        print("💾 Saving PDU results...")
        pdu_combined_data = []
        pdu_energy_summary = {}
        
        for hostname, (df, energy_dict) in pdu_results.items():
            if not df.empty:
                pdu_combined_data.append(df)
                pdu_energy_summary[hostname] = energy_dict
        
        if pdu_combined_data:
            pdu_combined_df = pd.concat(pdu_combined_data, ignore_index=True)
            
            # Convert timezone-aware timestamps to timezone-naive for Excel compatibility
            if 'timestamp' in pdu_combined_df.columns:
                pdu_combined_df['timestamp'] = pdu_combined_df['timestamp'].dt.tz_localize(None)
            
            # Save to Excel in output/irc_pdu folder
            output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'output', 'irc_pdu')
            os.makedirs(output_dir, exist_ok=True)
            pdu_filename = os.path.join(output_dir, f"pdu_power_analysis_{start_time.strftime('%Y%m%d_%H%M%S')}.xlsx")
            with pd.ExcelWriter(pdu_filename, engine='openpyxl') as writer:
                pdu_combined_df.to_excel(writer, sheet_name='Power_Data', index=False)
                
                # Create energy summary sheet with PDU total
                energy_summary_data = []
                total_pdu_energy = 0
                
                # Collect all data and calculate total PDU energy
                for hostname, energy_dict in pdu_energy_summary.items():
                    for metric, energy_kwh in energy_dict.items():
                        energy_summary_data.append({
                            'hostname': hostname,
                            'metric': metric,
                            'total_energy_kwh': energy_kwh
                        })
                        total_pdu_energy += energy_kwh
                
                # Add total PDU energy row
                if total_pdu_energy > 0:
                    energy_summary_data.append({
                        'hostname': '**ALL PDU TOTAL**',
                        'metric': 'pdu',
                        'total_energy_kwh': total_pdu_energy
                    })
                
                if energy_summary_data:
                    energy_summary_df = pd.DataFrame(energy_summary_data)
                    energy_summary_df.to_excel(writer, sheet_name='Energy_Summary', index=False)
            
            print(f"✅ PDU results saved to: {pdu_filename}")
            print(f"   📈 Total data points: {len(pdu_combined_df)}")
            print(f"   🔋 Total energy metrics: {len(energy_summary_data)}")
    
    print()
    print("🎉 Analysis complete!")
    print(f"📅 Finished at: {datetime.now()}")

if __name__ == "__main__":
    main()