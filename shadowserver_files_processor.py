import os
import re
import pandas as pd
from pathlib import Path

# === Paths ===
source_dir = r'd:\PD\shadow_intel_processor\src'
# Destination paths for different report types
destination_file = r'd:\PD\shadow_intel_processor\dst\destination.csv'
sinkhole_file = r'd:\PD\shadow_intel_processor\dst\sinkhole_events.csv'
blocklist_file = r'd:\PD\shadow_intel_processor\dst\blocklist_events.csv'

processed_log_path = r'd:\PD\shadow_intel_processor\dst\processed_files.txt'

# === Automatically create destination directory if it doesn't exist ===
destination_dir = os.path.dirname(destination_file)
if not os.path.exists(destination_dir):
    try:
        os.makedirs(destination_dir)
        print(f"Destination directory created at: {destination_dir}")
    except OSError as e:
        print(f"Error creating directory: {e}")

# === 1. VULNERABILITY REPORT CONFIGURATION ===
vuln_column_map = {
    'timestamp': 'Timestamp',
    'severity': 'Severity',
    'ip': 'IP',
    'protocol': 'Protocol',
    'port': 'Port',
    'hostname': 'Asset Name/Hostname',
    'region': 'Region',
    'city': 'City'
}

vuln_destination_columns = [
    'Timestamp', 'Severity', 'IP', 'Protocol', 'Port', 'State',
    'Asset Name/Hostname', 'Asset Type', 'Region', 'City', 'Issue',
    'Description', 'Recurring Issue', 'Client Awareness Training Needed',
    'Advisory Sent', 'Date Advisory Sent', 'Issue Resolved',
    'Date Issue Resolved', 'Contact Person', 'Contact Email'
]

# === 2. SINKHOLE REPORT CONFIGURATION ===
# Maps raw CSV headers to the columns requested by user
sinkhole_column_map = {
    'timestamp': 'timestamp', 'protocol': 'protocol', 'src_ip': 'src_ip', 'src_port': 'src_port',
    'src_asn': 'src_asn', 'src_geo': 'src_geo', 'src_region': 'src_region', 'src_city': 'src_city',
    'src_hostname': 'src_hostname', 'src_naics': 'src_naics', 'src_sector': 'src_sector',
    'device_vendor': 'device_vendor', 'device_type': 'device_type', 'device_model': 'device_model',
    'severity': 'severity', 'dst_ip': 'dst_ip', 'dst_port': 'dst_port', 'dst_asn': 'dst_asn',
    'dst_geo': 'dst_geo', 'dst_region': 'dst_region', 'dst_city': 'dst_city',
    'dst_hostname': 'dst_hostname', 'dst_naics': 'dst_naics', 'dst_sector': 'dst_sector',
    'public_source': 'public_source', 'infection': 'infection', 'family': 'family', 'tag': 'tag',
    'application': 'application', 'version': 'version', 'event_id': 'event_id', 'http_url': 'http_url',
    'http_host': 'http_host', 'http_agent': 'http_agent', 'forwarded_by': 'forwarded_by',
    'ssl_cipher': 'ssl_cipher', 'http_referer': 'http_referer', 'ssl_servername': 'ssl_servername',
    'machine_name': 'machine_name'
}

sinkhole_destination_columns = [
    "timestamp", "protocol", "src_ip", "src_port", "src_asn", "src_geo", "src_region", "src_city",
    "src_hostname", "src_naics", "src_sector", "device_vendor", "device_type", "device_model",
    "severity", "dst_ip", "dst_port", "dst_asn", "dst_geo", "dst_region", "dst_city",
    "dst_hostname", "dst_naics", "dst_sector", "public_source", "infection", "family", "tag",
    "application", "version", "event_id", "http_url", "http_host", "http_agent", "forwarded_by",
    "ssl_cipher", "http_referer", "ssl_servername", "machine_name",
    "State", "Issue", "Recurring Issue" # Added for processing logic
]

# === 3. BLOCKLIST REPORT CONFIGURATION ===
blocklist_column_map = {
    'timestamp': 'timestamp', 'severity': 'severity', 'ip': 'ip', 'hostname': 'hostname',
    'source': 'source', 'reason': 'reason', 'asn': 'asn', 'geo': 'geo',
    'region': 'region', 'city': 'city', 'naics': 'naics', 'sector': 'sector', 'tag': 'tag'
}

blocklist_destination_columns = [
    "timestamp", "severity", "ip", "hostname", "source", "reason", "asn", "geo",
    "region", "city", "naics", "sector", "tag",
    "State", "Issue", "Recurring Issue" # Added for processing logic
]


def extract_issue_from_filename(filename):
    """Extracts a clean issue name from the report's filename."""
    match = re.search(r'scan_([^-.]+)', filename)
    if match:
        return match.group(1).replace('_', ' ').strip().lower()
    return "unknown issue"

def load_processed_files():
    """Loads the set of already processed file names."""
    try:
        with open(processed_log_path, 'r') as f:
            return set(f.read().splitlines())
    except FileNotFoundError:
        return set()

def load_existing_data(filepath, columns):
    """Loads the existing CSV or returns an empty DataFrame structure with specific columns."""
    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath, dtype=str)
            # Simple cleanup of string columns
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            return df
        except Exception:
            pass # Fallback to empty if file is corrupt/unreadable
    
    # Return empty dataframe with correct columns
    return pd.DataFrame(columns=columns)

def get_col_name(df, candidates):
    """Helper to find which column name exists in the dataframe from a list of candidates."""
    for col in candidates:
        if col in df.columns:
            return col
    return None

def process_batch(file_list, target_file, category_name, log_callback):
    """
    Processes a specific list of files and saves them to the target destination.
    Returns the list of successfully processed filenames.
    """
    if not file_list:
        return []

    # Determine configuration based on category
    if category_name == "Sinkhole":
        current_map = sinkhole_column_map
        dest_columns = sinkhole_destination_columns
    elif category_name == "Blocklist":
        current_map = blocklist_column_map
        dest_columns = blocklist_destination_columns
    else:
        current_map = vuln_column_map
        dest_columns = vuln_destination_columns

    log_callback(f">> Processing {len(file_list)} {category_name} file(s)...")
    
    # 1. Load existing history
    df_dest = load_existing_data(target_file, dest_columns)
    
    # Ensure all required columns exist in destination
    for col in dest_columns:
        if col not in df_dest.columns:
            df_dest[col] = ""

    # Pre-process 'Recurring Issue' for calculation
    df_dest['Recurring Issue'] = pd.to_numeric(df_dest['Recurring Issue'], errors='coerce').fillna(0).astype(int)

    successful_files = []
    new_records_count = 0

    # Identify key columns for recurrence check (handling differences in headers)
    # Format: [List of possible names]
    keys_severity = ['Severity', 'severity']
    keys_ip = ['IP', 'src_ip', 'ip']
    keys_proto = ['Protocol', 'protocol']
    keys_port = ['Port', 'src_port', 'port'] # Blocklist might not have port, handle gracefully
    keys_timestamp = ['Timestamp', 'timestamp']

    # 2. Process each new file
    for filename in file_list:
        file_path = os.path.join(source_dir, filename)
        try:
            if filename.endswith('.csv'):
                df_src = pd.read_csv(file_path, dtype=str)
            else:
                df_src = pd.read_excel(file_path, dtype=str)
            
            df_src = df_src.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        except Exception as e:
            log_callback(f"[ERROR] Failed to read {filename}: {e}", "error")
            continue

        valid_cols = [col for col in current_map if col in df_src.columns]
        if not valid_cols:
            log_callback(f"[WARNING] Skipping {filename}: No matching columns found.", "warning")
            continue

        # Map columns to standard format
        temp = df_src[valid_cols].rename(columns={k: v for k, v in current_map.items() if k in df_src.columns})
        temp.dropna(how='all', inplace=True)
        
        if temp.empty:
            log_callback(f"[INFO] Skipping {filename}: File contains no data.", "info")
            successful_files.append(filename) # Mark empty files as processed so we don't retry
            continue

        # Fill missing destination columns
        for col in dest_columns:
            if col not in temp.columns:
                temp[col] = ""

        # Set Defaults
        temp['State'] = 'open'
        temp['Issue'] = extract_issue_from_filename(filename)
        temp['Recurring Issue'] = 0
        
        # Reorder columns
        temp = temp[dest_columns].copy()

        # --- Recurring Issue Logic ---
        # Find actual column names in the current dataframe
        col_sev = get_col_name(df_dest, keys_severity)
        col_ip = get_col_name(df_dest, keys_ip)
        col_proto = get_col_name(df_dest, keys_proto)
        col_port = get_col_name(df_dest, keys_port)
        col_time = get_col_name(df_dest, keys_timestamp)

        # Build query conditions dynamically based on available columns
        for i, new_row in temp.iterrows():
            conditions = []
            if col_sev: conditions.append(df_dest[col_sev] == new_row[col_sev])
            if col_ip: conditions.append(df_dest[col_ip] == new_row[col_ip])
            if col_proto: conditions.append(df_dest[col_proto] == new_row[col_proto])
            if col_port and col_port in df_dest.columns: conditions.append(df_dest[col_port] == new_row[col_port])
            
            # Always check state, issue, and timestamp difference
            conditions.append(df_dest['State'] == new_row['State'])
            conditions.append(df_dest['Issue'] == new_row['Issue'])
            if col_time: conditions.append(df_dest[col_time] != new_row[col_time])

            if conditions:
                # Combine all conditions with &
                final_condition = conditions[0]
                for cond in conditions[1:]:
                    final_condition = final_condition & cond
                
                matches = df_dest[final_condition]
                
                if not matches.empty:
                    temp.at[i, 'Recurring Issue'] = matches['Recurring Issue'].max() + 1

        # Update the main dataframe IMMEDIATELY so the next file in this loop 
        # sees these records when checking for recurrences
        df_dest = pd.concat([temp, df_dest], ignore_index=True)
        
        new_records_count += len(temp)
        successful_files.append(filename)

    if not successful_files:
        return []

    # 3. Final Sort and Save
    # Sort by timestamp descending (find the correct timestamp col name first)
    ts_col = get_col_name(df_dest, keys_timestamp)
    if ts_col:
        df_dest[ts_col] = pd.to_datetime(df_dest[ts_col], errors='coerce')
        df_dest = df_dest.sort_values(by=ts_col, ascending=False, na_position='last').reset_index(drop=True)

    try:
        df_dest.to_csv(target_file, index=False)
        log_callback(f"[SUCCESS] Saved {new_records_count} new records to: {os.path.basename(target_file)}", "success")
    except Exception as e:
        log_callback(f"[ERROR] Error saving {category_name} file: {e}", "error")
        return [] 

    return successful_files

def run_discover(log_callback):
    """Dry run to see what files are available."""
    log_callback("=== Running in Discover Mode ===")
    processed_files = load_processed_files()
    
    files_to_process = [f for f in os.listdir(source_dir) 
                        if (f.endswith('.csv') or f.endswith('.xlsx')) and f not in processed_files]

    if not files_to_process:
        log_callback("No new files to process.")
        return

    log_callback(f"Found {len(files_to_process)} new file(s).")
    
    # Categorize for reporting
    sinkhole_count = sum(1 for f in files_to_process if 'sinkhole' in f.lower())
    blocklist_count = sum(1 for f in files_to_process if 'blocklist' in f.lower())
    vuln_count = len(files_to_process) - sinkhole_count - blocklist_count
    
    log_callback(f"  > Sinkhole Reports: {sinkhole_count}")
    log_callback(f"  > Blocklist Reports: {blocklist_count}")
    log_callback(f"  > Vulnerability Reports: {vuln_count}")

def run_process(log_callback):
    """Main processing function called by GUI or CLI."""
    log_callback("=== Running in Process Mode ===")
    processed_files = load_processed_files()
    
    all_files = [f for f in os.listdir(source_dir) 
                 if (f.endswith('.csv') or f.endswith('.xlsx')) and f not in processed_files]
    
    if not all_files:
        log_callback("[INFO] No new files to process.")
        return

    # Sort files into buckets
    sinkhole_files = []
    blocklist_files = []
    vuln_files = []

    for f in all_files:
        lower_name = f.lower()
        if 'sinkhole' in lower_name:
            sinkhole_files.append(f)
        elif 'blocklist' in lower_name:
            blocklist_files.append(f)
        else:
            vuln_files.append(f)

    # Display Totals
    total_count = len(all_files)
    log_callback(f"Total files found for processing: {total_count}")
    if total_count > 0:
        log_callback(f"  > Sinkhole Reports: {len(sinkhole_files)}")
        log_callback(f"  > Blocklist Reports: {len(blocklist_files)}")
        log_callback(f"  > Vulnerability Reports: {len(vuln_files)}")
    
    log_callback("-" * 40)

    # Process buckets independently
    processed_successfully = []
    
    if vuln_files:
        processed_successfully.extend(process_batch(vuln_files, destination_file, "Vulnerability", log_callback))
    
    if sinkhole_files:
        processed_successfully.extend(process_batch(sinkhole_files, sinkhole_file, "Sinkhole", log_callback))
        
    if blocklist_files:
        processed_successfully.extend(process_batch(blocklist_files, blocklist_file, "Blocklist", log_callback))

    # Update log file with all successfully processed files
    if processed_successfully:
        try:
            with open(processed_log_path, 'a') as log:
                for fname in processed_successfully:
                    log.write(fname + '\n')
            log_callback("-" * 40)
            log_callback(f"[SUCCESS] Logged {len(processed_successfully)} files as processed.", "success")
        except Exception as e:
            log_callback(f"[ERROR] Failed to update processed file log: {e}", "error")

# CLI Entry Point
if __name__ == "__main__":
    def console_logger(msg, level="info"):
        # Simple formatting for CLI usage
        print(f"{msg}")
    
    print("Starting processor...")
    run_process(console_logger)