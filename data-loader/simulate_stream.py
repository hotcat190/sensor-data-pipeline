import os
import sys
import time
import shutil
import glob
import argparse

def main():
    parser = argparse.ArgumentParser(description="Sensor Ingestion Stream Simulator")
    parser.add_argument(
        "--mode", 
        choices=["streaming", "bulk"], 
        required=True, 
        help="streaming (real-time replay) or bulk (high-volume pressure test)"
    )
    args = parser.parse_args()

    # Determine paths relative to the script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    
    bme_dir = os.path.join(project_dir, "clickhouse-bme-data")
    sds_dir = os.path.join(project_dir, "clickhouse-sds-data")
    combined_dir = os.path.join(project_dir, "combined_sds")
    
    drop_zone_dir = os.path.join(project_dir, "input-drop-zone")
    os.makedirs(drop_zone_dir, exist_ok=True)

    print(f"Drop Zone: {drop_zone_dir}")

    if args.mode == "streaming":
        print("Starting streaming simulation (2017 & 2018 CSV files, 2s interval)...")
        
        # Gather BME CSV files from 2017/2018
        bme_files = []
        for year in ["2017", "2018"]:
            bme_files.extend(glob.glob(os.path.join(bme_dir, f"{year}-*.csv")))
            
        # Gather SDS CSV files from 2017/2018
        sds_files = []
        for year in ["2017", "2018"]:
            sds_files.extend(glob.glob(os.path.join(sds_dir, f"{year}-*.csv")))
            
        # Combine and sort chronologically by filename (which starts with date YYYY-MM-DD)
        all_files = sorted(bme_files + sds_files, key=os.path.basename)
        
        if not all_files:
            print("Error: No 2017 or 2018 CSV files found.")
            sys.exit(1)
            
        print(f"Found {len(all_files)} files to stream.")
        
        try:
            for idx, filepath in enumerate(all_files):
                filename = os.path.basename(filepath)
                dest_path = os.path.join(drop_zone_dir, filename)
                print(f"[{idx+1}/{len(all_files)}] Copying {filename} -> drop-zone...")
                shutil.copy2(filepath, dest_path)
                time.sleep(2)
        except KeyboardInterrupt:
            print("\nStreaming stopped by user.")
            
    elif args.mode == "bulk":
        print("Starting bulk loading simulation for Backpressure testing...")
        
        bme_bulk = os.path.join(combined_dir, "combined_bme.csv")
        sds_bulk = os.path.join(combined_dir, "combined_sds.csv")
        
        files_to_copy = []
        if os.path.exists(bme_bulk):
            files_to_copy.append(bme_bulk)
        else:
            print(f"Warning: Bulk BME file not found at {bme_bulk}")
            
        if os.path.exists(sds_bulk):
            files_to_copy.append(sds_bulk)
        else:
            print(f"Warning: Bulk SDS file not found at {sds_bulk}")
            
        if not files_to_copy:
            print("Error: No bulk combined CSV files found.")
            sys.exit(1)
            
        for filepath in files_to_copy:
            filename = os.path.basename(filepath)
            dest_path = os.path.join(drop_zone_dir, filename)
            print(f"Copying bulk file {filename} (~{os.path.getsize(filepath) / (1024*1024*1024):.2f} GB) to drop-zone...")
            shutil.copy2(filepath, dest_path)
            print(f"Successfully copied {filename}.")

if __name__ == "__main__":
    main()
