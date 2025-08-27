import os
import pandas as pd
from pathlib import Path

# === Configuration ===
source_dir = 'src'  # r'd:\PD\shadow_server\src'      Directory with source .csv/.xlsx files
destination_file = 'dst/destination.csv'  # r'd:\PD\shadow_server\dst\destination.csv'
processed_log_path = 'dst/processed_log.txt'  # r'd:\PD\shadow_server\dst\processed_files.txt'

# === Column Mapping from Source to Destination ===
column_map = {
    'timestamp': 'Timestamp',
    'severity': 'Severity',
    'ip': 'IP',
    'protocol': 'Protocol',
    'port': 'Port',
    'hostname': 'Asset Name/Hostname',
    'region': 'Region',
    'city': 'City',
}

# === Destination Columns (including custom ones) ===
destination_columns = [
    'Timestamp', 'Severity', 'IP', 'Protocol', 'Port', 'State', 'Asset Name/Hostname',
    'Asset Type', 'Region', 'City', 'Issue', 'Description', 'Recurring Issue',
    'Client Awareness Training Needed', 'Advisory Sent', 'Date Advisory Sent',
    'Issue Resolved', 'Date Issue Resolved', 'Contact Person', 'Contact Email'
]

# === Load existing destination file or initialize ===
if os.path.exists(destination_file):
    try:
        df_dest = pd.read_csv(destination_file, dtype=str)
        print(f"[INFO] Loaded existing destination file: {destination_file}")
    except Exception as e:
        print(f"[ERROR] Failed to read existing destination file: {e}")
        df_dest = pd.DataFrame(columns=destination_columns)
else:
    df_dest = pd.DataFrame(columns=destination_columns)

# Ensure all destination columns exist
for col in destination_columns:
    if col not in df_dest.columns:
        df_dest[col] = ""

# === Load processed file log ===
if os.path.exists(processed_log_path):
    with open(processed_log_path, 'r') as log_file:
        processed_files = set(line.strip() for line in log_file.readlines())
else:
    processed_files = set()

# === Process each file in source_dir ===
for file in os.listdir(source_dir):
    if not (file.endswith('.csv') or file.endswith('.xlsx')):
        continue

    filepath = os.path.join(source_dir, file)

    # Skip already processed files
    if file in processed_files:
        print(f"[SKIP] Already processed: {file}")
        continue

    print(f"[INFO] Processing new file: {file}")

    # === Read file ===
    try:
        if file.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str)
        else:
            df = pd.read_excel(filepath, dtype=str)
    except Exception as e:
        print(f"[ERROR] Failed to read file: {file} ({e})")
        continue

    # === Rename and Filter Columns ===
    df_renamed = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
    df_filtered = df_renamed[[v for v in column_map.values() if v in df_renamed.columns]]

    if df_filtered.empty:
        print(f"[WARN] No matching columns in: {file}")
        continue

    # Add missing destination columns
    for col in destination_columns:
        if col not in df_filtered.columns:
            df_filtered[col] = ""

    # === Set Default and Custom Values ===
    df_filtered['State'] = 'open'
    df_filtered['Issue'] = file.split('-scan_')[-1].split('-')[0].replace('_', ' ').strip().lower()
    df_filtered['Recurring Issue'] = 0  # Will update this below

    # Ensure correct column order
    df_filtered = df_filtered[destination_columns].copy()

    # === Recurring Issue Logic ===
    key_columns = ['Severity', 'IP', 'Protocol', 'Port', 'State', 'Issue']
    for col in key_columns:
        if col not in df_dest.columns:
            df_dest[col] = ""

    df_dest['Recurring Issue'] = pd.to_numeric(df_dest['Recurring Issue'], errors='coerce').fillna(0).astype(int)

    for i, new_row in df_filtered.iterrows():
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
            df_filtered.at[i, 'Recurring Issue'] = matches['Recurring Issue'].max() + 1

    # === Combine: New records on top ===
    df_dest = pd.concat([df_filtered, df_dest], ignore_index=True)

    # === Append to processed log ===
    with open(processed_log_path, 'a') as log_file:
        log_file.write(file + '\n')

# === Final Sort by Timestamp Desc ===
df_dest['Timestamp'] = pd.to_datetime(df_dest['Timestamp'], errors='coerce')
df_dest = df_dest.sort_values(by='Timestamp', ascending=False, na_position='last').reset_index(drop=True)

# === Save Result ===
df_dest.to_csv(destination_file, index=False)
print(f"[DONE] Destination file updated: {destination_file}")
