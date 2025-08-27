import os
import re
import pandas as pd
from pathlib import Path
import argparse

def extract_issue_from_filename(filename):
    """Extracts a clean issue name from the report's filename."""
    match = re.search(r'scan_([^-.]+)', filename)
    if match:
        return match.group(1).replace('_', ' ').strip().lower()
    return "unknown issue"

def run_discover_mode(source_dir, files_to_process):
    """
    Analyzes new files and reports findings without modifying anything.
    """
    print("\n--- Running in Discover Mode (Dry Run) ---")
    total_new_rows = 0
    files_with_issues = 0

    # In discover mode, it's useful to see each file being analyzed
    for filename in files_to_process:
        print(f"  -> Analyzing '{filename}'...")
        file_path = os.path.join(source_dir, filename)
        issue_found = False
        try:
            df_src = pd.read_csv(file_path, dtype=str) if filename.endswith('.csv') else pd.read_excel(file_path, dtype=str)
            if df_src.empty:
                print("     ⚠️ Issue: File is empty.")
                issue_found = True
            else:
                print(f"     Found {len(df_src)} potential new records.")
                total_new_rows += len(df_src)
        except Exception as e:
            print(f"     ❌ Issue: Could not read file. Reason: {e}")
            issue_found = True
        
        if issue_found:
            files_with_issues += 1
            
    print("\n--- Discovery Summary ---")
    print(f"Total potential new records: {total_new_rows} from {len(files_to_process)} files.")
    if files_with_issues > 0:
        print(f"Files with potential issues: {files_with_issues}")
    print("No files were changed.")


def run_process_mode(source_dir, files_to_process, destination_file, processed_log_path, column_map, destination_columns):
    """
    Processes new files and updates the destination CSV and log.
    """
    print("\n--- Running in Process Mode ---")
    
    # Load or create destination DataFrame
    try:
        df_dest = pd.read_csv(destination_file, dtype=str).apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    except FileNotFoundError:
        df_dest = pd.DataFrame(columns=destination_columns)
        print("Destination file not found. A new one will be created.")
    except Exception as e:
        df_dest = pd.DataFrame(columns=destination_columns)
        print(f"Warning: Could not read destination file, will create a new one. Reason: {e}")

    # Ensure all required columns exist
    for col in destination_columns:
        if col not in df_dest.columns:
            df_dest[col] = ""

    newly_processed_log = []
    all_new_data = []
    
    # Process the new files with a summary message instead of per-file messages
    print(f"⚙️  Processing {len(files_to_process)} new file(s)...")
    for filename in files_to_process:
        file_path = os.path.join(source_dir, filename)
        try:
            df_src = pd.read_csv(file_path, dtype=str) if filename.endswith('.csv') else pd.read_excel(file_path, dtype=str)
            df_src = df_src.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        except Exception:
            continue # Silently skip files that can't be read in process mode

        valid_cols = [col for col in column_map if col in df_src.columns]
        if not valid_cols: continue

        temp = df_src[valid_cols].rename(columns=column_map)
        temp.dropna(how='all', inplace=True)
        if temp.empty: continue

        for col in destination_columns:
            if col not in temp.columns:
                temp[col] = ""
        temp['State'] = 'open'
        temp['Issue'] = extract_issue_from_filename(filename)
        temp['Recurring Issue'] = 0

        all_new_data.append(temp[destination_columns].copy())
        newly_processed_log.append(filename)

    if not all_new_data:
        print("\nNo new data was extracted from the files.")
        return

    df_new = pd.concat(all_new_data, ignore_index=True)

    # Recurring Issue Detection
    df_dest['Recurring Issue'] = pd.to_numeric(df_dest['Recurring Issue'], errors='coerce').fillna(0).astype(int)
    key_columns = ['Severity', 'IP', 'Protocol', 'Port', 'State', 'Issue']

    for i, new_row in df_new.iterrows():
        if all(col in df_dest.columns for col in key_columns):
            matches = df_dest[
                (df_dest['Severity'] == new_row['Severity']) &
                (df_dest['IP'] == new_row['IP']) &
                (df_dest['Protocol'] == new_row['Protocol']) &
                (df_dest['Port'] == new_row['Port']) &
                (df_dest['State'] == new_row['State']) &
                (df_dest['Issue'] == new_row['Issue']) &
                (df_dest['Timestamp'] != new_row['Timestamp'])
            ]
            if not matches.empty:
                df_new.at[i, 'Recurring Issue'] = matches['Recurring Issue'].max() + 1

    # Combine, Sort, and Save
    df_final = pd.concat([df_new, df_dest], ignore_index=True)
    df_final['Timestamp'] = pd.to_datetime(df_final['Timestamp'], errors='coerce')
    df_final = df_final.sort_values(by='Timestamp', ascending=False, na_position='last').reset_index(drop=True)

    try:
        df_final.to_csv(destination_file, index=False)
        print(f"\n✅ Destination file updated successfully with {len(df_new)} new records.")
    except Exception as e:
        print(f"\n❌ Error saving destination file: {e}")

    # Update Processed Log
    if newly_processed_log:
        try:
            with open(processed_log_path, 'a') as log:
                log.write('\n'.join(newly_processed_log) + '\n')
            print(f"✅ Logged {len(newly_processed_log)} new files to {processed_log_path}")
        except Exception as e:
            print(f"❌ Failed to update processed file log: {e}")

def main():
    """Parses command-line arguments and runs the selected mode."""
    parser = argparse.ArgumentParser(description="Process report files into a master CSV.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "mode", choices=['discover', 'process'], nargs='?', default='process',
        help=("The operational mode:\n"
              "  discover - (Dry Run) Shows which new files would be processed.\n"
              "  process  - (Default) Processes new files and updates the destination CSV.")
    )
    args = parser.parse_args()

    # Shared Setup
    source_dir = r'd:\PD\shadow_server\src'
    destination_file = r'd:\PD\shadow_server\dst\destination.csv'
    processed_log_path = r'd:\PD\shadow_server\dst\processed_files.txt'
    
    column_map = {'timestamp': 'Timestamp', 'severity': 'Severity', 'ip': 'IP', 'protocol': 'Protocol', 'port': 'Port', 'hostname': 'Asset Name/Hostname', 'region': 'Region', 'city': 'City'}
    destination_columns = ['Timestamp', 'Severity', 'IP', 'Protocol', 'Port', 'State', 'Asset Name/Hostname', 'Asset Type', 'Region', 'City', 'Issue', 'Description', 'Recurring Issue', 'Client Awareness Training Needed', 'Advisory Sent', 'Date Advisory Sent', 'Issue Resolved', 'Date Issue Resolved', 'Contact Person', 'Contact Email']

    try:
        with open(processed_log_path, 'r') as f:
            processed_files = set(f.read().splitlines())
    except FileNotFoundError:
        processed_files = set()

    # --- New, user-friendly summary at the start ---
    all_source_files = [f for f in os.listdir(source_dir) if f.endswith(('.csv', '.xlsx'))]
    files_to_process = [f for f in all_source_files if f not in processed_files]
    num_skipped = len(all_source_files) - len(files_to_process)

    print(f"🔍 Found {len(all_source_files)} total files in source directory.")
    if num_skipped > 0:
        print(f"⏭️  Skipping {num_skipped} already processed files.")

    if not files_to_process:
        print("✅ No new files to process.")
        return
    
    # Execute Selected Mode
    if args.mode == 'discover':
        run_discover_mode(source_dir, files_to_process)
    else: # process mode
        run_process_mode(source_dir, files_to_process, destination_file, processed_log_path, column_map, destination_columns)

if __name__ == "__main__":
    main()