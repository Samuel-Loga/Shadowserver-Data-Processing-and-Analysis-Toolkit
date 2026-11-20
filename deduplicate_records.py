import pandas as pd
import os
import argparse

def get_dynamic_col(df, candidates):
    """Finds the first existing column from a list of candidates (case-insensitive)."""
    lower_cols = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_cols:
            return lower_cols[candidate.lower()]
    return None

def deduplicate_and_update(source_file, output_file):
    if not os.path.exists(source_file):
        print(f"[ERROR] Source file not found: '{source_file}'")
        return

    try:
        print(f"[INFO] Loading data from '{source_file}'...")
        df = pd.read_csv(source_file, dtype=str, low_memory=False)
    except Exception as e:
        print(f"[ERROR] {e}")
        return

    # 1. Identify key columns dynamically
    ts_col = get_dynamic_col(df, ['timestamp', 'time'])
    ip_col = get_dynamic_col(df, ['src_ip', 'ip', 'dst_ip', 'source_ip'])
    port_col = get_dynamic_col(df, ['src_port', 'port', 'dst_port'])
    proto_col = get_dynamic_col(df, ['protocol', 'proto'])
    sev_col = get_dynamic_col(df, ['severity', 'level'])
    state_col = get_dynamic_col(df, ['state'])
    issue_col = get_dynamic_col(df, ['issue'])

    if not ts_col:
        print(f"[ERROR] No timestamp column found in {source_file}. Cannot safely deduplicate.")
        return

    # Build grouping keys based on what actually exists in this specific file
    key_columns = []
    for col in [ip_col, port_col, proto_col, sev_col, state_col, issue_col]:
        if col: key_columns.append(col)

    if not key_columns:
        print(f"[ERROR] No identifiable key columns found (e.g., IP, Port, Issue) to group by.")
        return

    print(f"[INFO] Deduplicating dynamically based on columns: {key_columns}")
    
    # Formatting
    df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
    rec_col = get_dynamic_col(df, ['recurring issue', 'Recurring Issue'])
    if rec_col:
        df[rec_col] = pd.to_numeric(df[rec_col], errors='coerce').fillna(0).astype(int)

    # Drop rows where all key identifiers are missing
    df.dropna(subset=key_columns + [ts_col], inplace=True)

    print("[INFO] Sorting records by timestamp...")
    df_sorted = df.sort_values(by=key_columns + [ts_col], ascending=[True]*len(key_columns) + [False])

    grouped = df_sorted.groupby(key_columns)
    final_records = []
    
    print(f"[INFO] Processing {len(grouped)} unique issue groups...")
    
    for _, group in grouped:
        latest_record = group.iloc[0].copy()
        num_duplicates = len(group) - 1
        if num_duplicates > 0 and rec_col:
            latest_record[rec_col] += num_duplicates
        final_records.append(latest_record)

    if not final_records:
        print("[WARNING] No records found after grouping.")
        return
        
    df_final = pd.DataFrame(final_records)
    df_final = df_final.sort_values(by=ts_col, ascending=False)
    
    output_dir = os.path.dirname(os.path.abspath(output_file))
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"[INFO] Created output directory: '{output_dir}'")

    df_final.to_csv(output_file, index=False)
    print(f"[SUCCESS] Removed {len(df) - len(df_final)} duplicates. Saved to '{output_file}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamically Deduplicate Shadowserver CSV reports.")
    parser.add_argument("source_file", help="Input CSV file")
    parser.add_argument("output_file", help="Output CSV file")
    
    # We leave the type flag just in case older scripts call it, but ignore it.
    parser.add_argument("-t", "--type", required=False, help="[Deprecated] Schema is auto-detected.")
    
    args = parser.parse_args()
    deduplicate_and_update(args.source_file, args.output_file)