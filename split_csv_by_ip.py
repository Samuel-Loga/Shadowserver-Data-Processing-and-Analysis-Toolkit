import pandas as pd
import os
import argparse

def get_dynamic_ip_col(df):
    """Finds the primary IP column in the dataframe."""
    candidates = ['src_ip', 'ip', 'dst_ip', 'source_ip']
    lower_cols = {col.lower(): col for col in df.columns}
    
    for candidate in candidates:
        if candidate in lower_cols:
            return lower_cols[candidate]
    return None


def split_all_csvs(source_dir, output_dir):
    """
    Splits all CSV files in source_dir into dedicated report subfolders inside output_dir.
    """
    if not os.path.exists(source_dir):
        print(f"[ERROR] Source directory not found at: {source_dir}")
        return

    csv_files = sorted([
        f for f in os.listdir(source_dir)
        if f.lower().endswith('.csv') and os.path.isfile(os.path.join(source_dir, f))
    ])

    if not csv_files:
        print(f"[INFO] No CSV files found in '{source_dir}' to split.")
        return

    print(f"[INFO] Found {len(csv_files)} CSV file(s) in '{source_dir}'. Splitting all...")
    for filename in csv_files:
        file_path = os.path.join(source_dir, filename)
        report_subfolder = os.path.splitext(filename)[0]
        target_subdir = os.path.join(output_dir, report_subfolder)
        print("-" * 50)
        split_csv_by_ip_prefix(file_path, target_subdir)

    print("\n" + "=" * 50)
    print(f"[SUCCESS] Finished splitting all {len(csv_files)} file(s) into '{output_dir}'.")


def split_csv_by_ip_prefix(source_file, output_dir):
    """
    Splits a CSV file dynamically based on the first 3 octets of the primary IP address.
    Supports a single CSV path, a directory path, or 'all' (for all CSVs in dst/).
    """
    cleaned_source = str(source_file).strip().strip('"').strip("'")

    if cleaned_source.lower() == 'all':
        base_dir = os.path.dirname(os.path.abspath(__file__))
        default_dst_dir = os.path.join(base_dir, 'dst')
        split_all_csvs(default_dst_dir, output_dir)
        return

    if os.path.isdir(cleaned_source):
        split_all_csvs(cleaned_source, output_dir)
        return

    if not os.path.exists(cleaned_source):
        print(f"[ERROR] Source file not found at: {cleaned_source}")
        return

    try:
        os.makedirs(output_dir, exist_ok=True)
        print(f"[INFO] Output directory: {output_dir}")

        print(f"[INFO] Reading source file: {cleaned_source}...")
        df = pd.read_csv(cleaned_source, dtype=str, low_memory=False)
        
        # Clean column names (strip whitespace)
        df.columns = [col.strip() for col in df.columns]
        
        ip_col = get_dynamic_ip_col(df)
        
        if not ip_col:
            print(f"[ERROR] No valid IP column found (checked for src_ip, ip, dst_ip).")
            print(f"   Available columns: {list(df.columns)}")
            return
            
        print(f"[INFO] Auto-detected IP column: '{ip_col}'")
            
    except Exception as e:
        print(f"[ERROR] Reading CSV: {e}")
        return

    def get_ip_prefix(ip_val):
        # Handle non-string or malformed IPs
        if not isinstance(ip_val, str) or ip_val.count('.') < 2: 
            return "invalid_ip"
        # Extract first 3 octets (e.g., 192.168.1)
        return ".".join(ip_val.split('.')[:3])

    print("[INFO] Generating IP prefixes...")
    # Create a temporary column for grouping
    df['temp_ip_prefix'] = df[ip_col].apply(get_ip_prefix)
    
    grouped = df.groupby('temp_ip_prefix')
    
    total_groups = len(grouped)
    print(f"[INFO] Found {total_groups} unique prefixes. Writing files...")
    
    count = 0
    for prefix, group_df in grouped:
        if prefix == "invalid_ip":
            filename = "records_with_invalid_ips.csv"
        else:
            filename = f"{prefix.replace('.', '_')}.csv"
            
        output_path = os.path.join(output_dir, filename)
        
        try:
            # Drop the temp column before saving
            group_df.drop(columns=['temp_ip_prefix']).to_csv(output_path, index=False)
            count += 1
        except Exception as e:
            print(f"[ERROR] Failed to save {filename}: {e}")

    print(f"\n[SUCCESS] Splitting complete! Created {count} files in '{output_dir}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split Shadowserver CSV reports by IP subnet.")
    
    parser.add_argument(
        "source_file",
        help="Path to a specific CSV file, a directory, or 'all' to split all CSVs in dst/."
    )
    parser.add_argument("output_dir", help="Directory where split files will be saved.")
    
    # Kept for backwards compatibility if old scripts run with this flag
    parser.add_argument("-t", "--type", required=False, help="[Deprecated] IP column is auto-detected.")
    
    args = parser.parse_args()
    
    split_csv_by_ip_prefix(args.source_file, args.output_dir)