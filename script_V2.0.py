import os
import re
import pandas as pd
from pathlib import Path

# Paths
source_folder = r'd:\PD\shadow_server\src'
destination_file = r'd:\PD\shadow_server\dst\destination.csv'
processed_log = r'd:\PD\shadow_server\dst\processed_files.txt'

# Mapping of source → destination columns
column_map = {
    'timestamp': 'Timestamp',
    'severity': 'Severity',
    'ip': 'IP',
    'protocol': 'Protocol',
    'port': 'Port',
    'hostname': 'Asset Name/Hostname',
    'region': 'Region',
    'city': 'City'
}

# Full destination file header
destination_columns = [
    'Timestamp', 'Severity', 'IP', 'Protocol', 'Port', 'State',
    'Asset Name/Hostname', 'Asset Type', 'Region', 'City', 'Issue',
    'Description', 'Recurring Issue', 'Client Awareness Training Needed',
    'Advisory Sent', 'Date Advisory Sent', 'Issue Resolved',
    'Date Issue Resolved', 'Contact Person', 'Contact Email'
]

# Function to extract "Issue" from filename
def extract_issue_from_filename(filename):
    match = re.search(r'scan_([^-.]+)', filename)
    if match:
        return match.group(1).replace('_', ' ').strip()
    return None

# Load list of already processed files
if os.path.exists(processed_log):
    with open(processed_log, 'r') as f:
        processed_files = set(f.read().splitlines())
else:
    processed_files = set()

# Load or create destination file
if os.path.exists(destination_file):
    try:
        df_dest = pd.read_csv(destination_file, dtype=str).apply(
            lambda x: x.str.strip() if x.dtype == "object" else x
        )
        print(f"Loaded existing destination file: {destination_file}")
    except Exception as e:
        print(f"Failed to read existing destination file: {e}")
        df_dest = pd.DataFrame(columns=destination_columns)
else:
    df_dest = pd.DataFrame(columns=destination_columns)

# Track newly processed files to append to log later
newly_processed = []

# Process new source files
for filename in os.listdir(source_folder):
    if not filename.endswith('.csv'):
        continue
    if filename in processed_files:
        print(f"Skipping already processed file: {filename}")
        continue

    file_path = os.path.join(source_folder, filename)
    print(f"Processing new file: {filename}")

    try:
        df_src = pd.read_csv(file_path, dtype=str).apply(
            lambda x: x.str.strip() if x.dtype == "object" else x
        )
    except Exception as e:
        print(f"Failed to read {filename}: {e}")
        continue

    valid_cols = [col for col in column_map if col in df_src.columns]
    if not valid_cols:
        print(f"No matching columns in: {filename}")
        continue

    # Rename and select valid columns
    temp = df_src[valid_cols].rename(columns={k: v for k, v in column_map.items() if k in df_src.columns})
    temp.dropna(how='all', inplace=True)
    if temp.empty:
        print(f"No usable data in: {filename}")
        continue

    # Fill missing columns with None
    for col in destination_columns:
        if col not in temp.columns:
            temp[col] = None

    # Set defaults
    temp['State'] = 'Open'
    temp['Issue'] = extract_issue_from_filename(filename)

    # Reorder
    temp = temp[destination_columns]

    # Append new rows at the top
    df_dest = pd.concat([temp, df_dest], ignore_index=True)

    # Ensure 'Timestamp' is datetime for proper sorting
    df_dest['Timestamp'] = pd.to_datetime(df_dest['Timestamp'], errors='coerce')

    # Sort by Timestamp descending (newest first)
    df_dest = df_dest.sort_values(by='Timestamp', ascending=False, na_position='last').reset_index(drop=True)

    # Mark file as processed
    newly_processed.append(filename)

# Write updated destination file
try:
    df_dest.to_csv(destination_file, index=False)
    print(f"\nDestination file updated: {destination_file}")
except Exception as e:
    print(f"Error saving destination: {e}")

# Update processed log
if newly_processed:
    with open(processed_log, 'a') as f:
        for fname in newly_processed:
            f.write(fname + '\n')
    print(f"Logged {len(newly_processed)} new files to {processed_log}")
