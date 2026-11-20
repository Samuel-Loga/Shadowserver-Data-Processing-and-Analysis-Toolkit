import os
import re
import pandas as pd
from pathlib import Path

# === Paths (Cross-Platform) ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.join(BASE_DIR, 'src')
dst_root = os.path.join(BASE_DIR, 'dst')
splitted_dir = os.path.join(BASE_DIR, 'splitted')
reports_dir = os.path.join(BASE_DIR, 'reports')
processed_log_path = os.path.join(dst_root, 'processed_files.txt')


def ensure_directories(log_callback=None):
    """Creates all required workspace directories if they do not exist."""
    for directory in [source_dir, dst_root, splitted_dir, reports_dir]:
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                msg = f"[INFO] Created missing directory: {directory}"
                if log_callback:
                    log_callback(msg)
                else:
                    print(msg)
            except OSError as e:
                err_msg = f"[ERROR] Failed to create directory {directory}: {e}"
                if log_callback:
                    log_callback(err_msg, "error")
                else:
                    print(err_msg)


# Ensure required directories exist on module load
ensure_directories()

# Standard tracking columns to append to ALL report types
TRACKING_COLUMNS = ['State', 'Issue', 'Recurring Issue']

def extract_type_and_issue(filename):
    """Dynamically extracts report type and clean issue name from filename."""
    # Matches standard format: YYYY-MM-DD-scan_name-105... OR YYYY-MM-DD-name-asn...
    type_match = re.search(r'\d{4}-\d{2}-\d{2}-(?:scan_)?(.*?)-(?:105|asn|malawi)', filename)
    
    if type_match:
        report_type = type_match.group(1).lower()
    else:
        # Fallback for weird filenames
        parts = filename.replace('.csv', '').replace('.xlsx', '').split('-')
        report_type = parts[3] if len(parts) > 3 else "unknown_report"
    
    issue_name = report_type.replace('_', ' ').strip()
    return report_type, issue_name

def load_processed_files():
    try:
        with open(processed_log_path, 'r') as f:
            return set(f.read().splitlines())
    except FileNotFoundError:
        return set()

def get_dynamic_col(df, candidates):
    """Finds the first existing column from a list of candidates (case-insensitive)."""
    lower_cols = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_cols:
            return lower_cols[candidate.lower()]
    return None

def process_dynamic_batch(file_list, log_callback):
    """Processes files dynamically based on their extracted report type."""
    if not file_list:
        return []

    # Group files by their dynamic report type
    files_by_type = {}
    for filename in file_list:
        report_type, issue_name = extract_type_and_issue(filename)
        if report_type not in files_by_type:
            files_by_type[report_type] = []
        files_by_type[report_type].append((filename, issue_name))

    successful_files = []
    
    # Process each report type independently
    for report_type, files in files_by_type.items():
        log_callback(f">> Processing {len(files)} file(s) for type: '{report_type}'")
        
        target_file = os.path.join(dst_root, f"{report_type}_events.csv")
        
        # Load existing history if it exists
        if os.path.exists(target_file):
            df_dest = pd.read_csv(target_file, dtype=str, low_memory=False)
            df_dest = df_dest.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        else:
            df_dest = pd.DataFrame()

        new_records_count = 0

        # Candidates for Recurrence Logic
        keys_time = ['timestamp', 'Timestamp', 'time']
        keys_ip = ['src_ip', 'ip', 'IP', 'dst_ip', 'source_ip']
        keys_port = ['src_port', 'port', 'Port', 'dst_port']
        keys_proto = ['protocol', 'Protocol', 'proto']
        keys_sev = ['severity', 'Severity', 'level']

        for filename, issue_name in files:
            file_path = os.path.join(source_dir, filename)
            try:
                if filename.endswith('.csv'):
                    df_src = pd.read_csv(file_path, dtype=str, low_memory=False)
                else:
                    df_src = pd.read_excel(file_path, dtype=str)
                df_src = df_src.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            except Exception as e:
                log_callback(f"[ERROR] Failed to read {filename}: {e}", "error")
                continue

            if df_src.empty:
                log_callback(f"[INFO] Skipping {filename}: Empty data.", "info")
                successful_files.append(filename)
                continue

            # Standardize column headers to lowercase to prevent duplicates (e.g., 'IP' vs 'ip')
            df_src.columns = [str(col).strip().lower() for col in df_src.columns]
            
            # If df_dest is empty (new file type), initialize it with the source columns
            if df_dest.empty:
                df_dest = pd.DataFrame(columns=list(df_src.columns) + [c.lower() for c in TRACKING_COLUMNS])

            # Add tracking columns
            df_src['state'] = 'open'
            df_src['issue'] = issue_name
            df_src['recurring issue'] = 0

            # --- Dynamic Recurring Issue Logic ---
            col_time = get_dynamic_col(df_dest, keys_time)
            col_ip = get_dynamic_col(df_dest, keys_ip)
            col_port = get_dynamic_col(df_dest, keys_port)
            col_proto = get_dynamic_col(df_dest, keys_proto)
            col_sev = get_dynamic_col(df_dest, keys_sev)

            if not df_dest.empty and col_ip:
                # Pre-process numeric for comparison
                df_dest['recurring issue'] = pd.to_numeric(df_dest['recurring issue'], errors='coerce').fillna(0).astype(int)
                
                for i, new_row in df_src.iterrows():
                    conditions = []
                    
                    if col_ip and col_ip in df_src.columns: conditions.append(df_dest[col_ip] == new_row[col_ip])
                    if col_port and col_port in df_src.columns: conditions.append(df_dest[col_port] == new_row[col_port])
                    if col_proto and col_proto in df_src.columns: conditions.append(df_dest[col_proto] == new_row[col_proto])
                    if col_sev and col_sev in df_src.columns: conditions.append(df_dest[col_sev] == new_row[col_sev])
                    
                    if 'state' in df_dest.columns: conditions.append(df_dest['state'] == new_row['state'])
                    if 'issue' in df_dest.columns: conditions.append(df_dest['issue'] == new_row['issue'])
                    
                    # Prevent matching the exact same row by time
                    if col_time and col_time in df_src.columns: conditions.append(df_dest[col_time] != new_row[col_time])

                    if conditions:
                        final_condition = conditions[0]
                        for cond in conditions[1:]:
                            final_condition = final_condition & cond
                        
                        matches = df_dest[final_condition]
                        if not matches.empty:
                            df_src.at[i, 'recurring issue'] = matches['recurring issue'].max() + 1

            # Append new rows to destination dataframe (pandas aligns columns automatically)
            df_dest = pd.concat([df_src, df_dest], ignore_index=True)
            new_records_count += len(df_src)
            successful_files.append(filename)

        # Final Sort and Save for this report type
        if not df_dest.empty:
            ts_col = get_dynamic_col(df_dest, keys_time)
            if ts_col:
                df_dest[ts_col] = pd.to_datetime(df_dest[ts_col], errors='coerce')
                df_dest = df_dest.sort_values(by=ts_col, ascending=False, na_position='last').reset_index(drop=True)

            try:
                os.makedirs(os.path.dirname(target_file), exist_ok=True)
                df_dest.to_csv(target_file, index=False)
                log_callback(f"[SUCCESS] Saved {new_records_count} new records to: {os.path.basename(target_file)}", "success")
            except Exception as e:
                log_callback(f"[ERROR] Saving {report_type}: {e}", "error")

    return successful_files

def run_discover(log_callback):
    """Dry run to see what files are available and their dynamic types."""
    log_callback("=== Running in Discover Mode (Dynamic) ===")
    ensure_directories(log_callback)
    processed_files = load_processed_files()
    
    files_to_process = [f for f in os.listdir(source_dir) 
                        if (f.endswith('.csv') or f.endswith('.xlsx')) and f not in processed_files]

    if not files_to_process:
        log_callback("No new files to process.")
        return

    log_callback(f"Found {len(files_to_process)} new file(s).")
    
    # Categorize dynamically
    types_found = {}
    for f in files_to_process:
        rtype, _ = extract_type_and_issue(f)
        types_found[rtype] = types_found.get(rtype, 0) + 1
        
    for rtype, count in types_found.items():
        log_callback(f"  > {rtype}: {count} files")

def run_process(log_callback):
    """Main processing function."""
    log_callback("=== Running in Process Mode (Dynamic) ===")
    ensure_directories(log_callback)
    processed_files = load_processed_files()
    
    all_files = [f for f in os.listdir(source_dir) 
                 if (f.endswith('.csv') or f.endswith('.xlsx')) and f not in processed_files]
    
    if not all_files:
        log_callback("[INFO] No new files to process.")
        return

    log_callback(f"Total files to process: {len(all_files)}")
    log_callback("-" * 40)

    # Process all files dynamically
    processed_successfully = process_dynamic_batch(all_files, log_callback)

    # Log Success
    if processed_successfully:
        try:
            with open(processed_log_path, 'a') as log:
                for fname in processed_successfully:
                    log.write(fname + '\n')
            log_callback("-" * 40)
            log_callback(f"[SUCCESS] Logged {len(processed_successfully)} processed files.", "success")
        except Exception as e:
            log_callback(f"[ERROR] Updating log file: {e}", "error")

if __name__ == "__main__":
    def console_logger(msg, level="info"):
        print(f"{msg}")
    print("Starting dynamic processor...")
    run_process(console_logger)