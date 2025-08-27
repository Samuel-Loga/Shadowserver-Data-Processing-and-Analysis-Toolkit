import os
import pandas as pd
from pathlib import Path

# Directory containing source files
source_dir = 'src'
destination_file = 'dst/destination.csv'

# Column mapping between source and destination
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

# Destination columns including custom ones
destination_columns = [
    'Timestamp', 'Severity', 'IP', 'Protocol', 'Port', 'State', 'Asset Name/Hostname',
    'Asset Type', 'Region', 'City', 'Issue', 'Description', 'Recurring Issue',
    'Client Awareness Training Needed', 'Advisory Sent', 'Date Advisory Sent',
    'Issue Resolved', 'Date Issue Resolved', 'Contact Person', 'Contact Email'
]

# Load existing destination file or initialize an empty DataFrame
if os.path.exists(destination_file):
    try:
        df_dest = pd.read_csv(destination_file, dtype=str)
        print(f"[INFO] Loaded existing destination file: {destination_file}")
    except Exception as e:
        print(f"[ERROR] Failed to read existing destination file: {e}")
        df_dest = pd.DataFrame(columns=destination_columns)
else:
    df_dest = pd.DataFrame(columns=destination_columns)

# Fill missing columns if destination was just created or is incomplete
for col in destination_columns:
    if col not in df_dest.columns:
        df_dest[col] = ""

# Process each file in the source directory
for file in os.listdir(source_dir):
    if not (file.endswith('.csv') or file.endswith('.xlsx')):
        continue

    filepath = os.path.join(source_dir, file)

    # Skip already processed files based on filename marker in 'Issue' column
    if any(df_dest['Issue'].str.contains(file)):
        print(f"[SKIP] Already processed: {file}")
        continue

    print(f"[INFO] Processing new file: {file}")

    # Read file based on extension
    try:
        if file.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str)
        else:
            df = pd.read_excel(filepath, dtype=str)
    except Exception as e:
        print(f"[ERROR] Failed to read file: {file} ({e})")
        continue

    # Rename columns according to mapping
    df_renamed = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    # Keep only the relevant destination columns
    df_filtered = df_renamed[[col for col in column_map.values() if col in df_renamed.columns]]

    if df_filtered.empty:
        print(f"[WARN] No matching columns in: {file}")
        continue

    # Add missing destination columns and fill defaults
    for col in destination_columns:
        if col not in df_filtered.columns:
            df_filtered[col] = ""

    # Set default values
    df_filtered['State'] = 'open'
    df_filtered['Issue'] = file.split('-scan_')[-1].split('-')[0].replace('_', ' ').strip().lower()
    df_filtered['Recurring Issue'] = 0

    # Ensure consistent columns and types
    df_filtered = df_filtered[destination_columns].copy()

    # Apply recurring issue logic
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

    # Combine with existing, putting new rows on top
    df_dest = pd.concat([df_filtered, df_dest], ignore_index=True)

# Sort by Timestamp descending (latest first)
df_dest['Timestamp'] = pd.to_datetime(df_dest['Timestamp'], errors='coerce')
df_dest = df_dest.sort_values(by='Timestamp', ascending=False, na_position='last').reset_index(drop=True)

# Save to destination
df_dest.to_csv(destination_file, index=False)
print(f"[DONE] Updated destination file: {destination_file}")
