import os
import re
import pandas as pd
from pathlib import Path

# === Paths ===
source_dir = r'd:\PD\shadow_intel_processor\src'
destination_file = r'd:\PD\shadow_intel_processor\dst\destination.csv'
processed_log_path = r'd:\PD\shadow_intel_processor\dst\processed_files.txt'

# === Automatically create destination directory if it doesn't exist ===
destination_dir = os.path.dirname(destination_file)
if not os.path.exists(destination_dir):
    print(f"Destination directory not found. Creating it at: {destination_dir}")
    os.makedirs(destination_dir)

# === Mapping: Source → Destination Columns ===
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

# === Full Destination Header ===
destination_columns = [
    'Timestamp', 'Severity', 'IP', 'Protocol', 'Port', 'State',
    'Asset Name/Hostname', 'Asset Type', 'Region', 'City', 'Issue',
    'Description', 'Recurring Issue', 'Client Awareness Training Needed',
    'Advisory Sent', 'Date Advisory Sent', 'Issue Resolved',
    'Date Issue Resolved', 'Contact Person', 'Contact Email'
]

# === Extract issue name from filename ===
def extract_issue_from_filename(filename):
    match = re.search(r'scan_([^-.]+)', filename)
    if match:
        return match.group(1).replace('_', ' ').strip().lower()
    return None

# === Load already processed files ===
try:
    with open(processed_log_path, 'r') as f:
        processed_files = set(f.read().splitlines())
except FileNotFoundError:
    processed_files = set()

# === Load destination or create fresh ===
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

# Ensure all required columns exist
for col in destination_columns:
    if col not in df_dest.columns:
        df_dest[col] = ""

# === Track new files processed this run ===
newly_processed = []

# === Process each file ===
for filename in os.listdir(source_dir):
    if not (filename.endswith('.csv') or filename.endswith('.xlsx')):
        continue
    if filename in processed_files:
        print(f"Skipping already processed file: {filename}")
        continue

    print(f"Processing new file: {filename}")
    file_path = os.path.join(source_dir, filename)

    # Read source
    try:
        if filename.endswith('.csv'):
            df_src = pd.read_csv(file_path, dtype=str)
        else:
            df_src = pd.read_excel(file_path, dtype=str)
        df_src = df_src.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    except Exception as e:
        print(f"Failed to read {filename}: {e}")
        continue

    # Select and rename relevant columns
    valid_cols = [col for col in column_map if col in df_src.columns]
    if not valid_cols:
        print(f"No matching columns in: {filename}")
        continue

    temp = df_src[valid_cols].rename(columns={k: v for k, v in column_map.items() if k in df_src.columns})
    temp.dropna(how='all', inplace=True)
    if temp.empty:
        print(f"No usable data in: {filename}")
        continue

    # Add missing destination columns
    for col in destination_columns:
        if col not in temp.columns:
            temp[col] = ""

    # Set default values
    temp['State'] = 'open'
    temp['Issue'] = extract_issue_from_filename(filename)
    temp['Recurring Issue'] = 0

    # Ensure correct column order
    temp = temp[destination_columns].copy()

    # === Recurring Issue Detection ===
    key_columns = ['Severity', 'IP', 'Protocol', 'Port', 'State', 'Issue']
    for col in key_columns:
        if col not in df_dest.columns:
            df_dest[col] = ""
    df_dest['Recurring Issue'] = pd.to_numeric(df_dest['Recurring Issue'], errors='coerce').fillna(0).astype(int)

    for i, new_row in temp.iterrows():
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
            temp.at[i, 'Recurring Issue'] = matches['Recurring Issue'].max() + 1

    # Combine new rows at top
    df_dest = pd.concat([temp, df_dest], ignore_index=True)

    # Mark file for logging
    newly_processed.append(filename)

# Final sort by Timestamp descending
df_dest['Timestamp'] = pd.to_datetime(df_dest['Timestamp'], errors='coerce')
df_dest = df_dest.sort_values(by='Timestamp', ascending=False, na_position='last').reset_index(drop=True)

# Save destination
try:
    df_dest.to_csv(destination_file, index=False)
    print(f"\nDestination file updated: {destination_file}")
except Exception as e:
    print(f"Error saving destination file: {e}")

# Update processed log
if newly_processed:
    try:
        with open(processed_log_path, 'a') as log:
            for fname in newly_processed:
                log.write(fname + '\n')
        print(f"Logged {len(newly_processed)} new files to {processed_log_path}")
    except Exception as e:
        print(f"Failed to update processed file log: {e}")
