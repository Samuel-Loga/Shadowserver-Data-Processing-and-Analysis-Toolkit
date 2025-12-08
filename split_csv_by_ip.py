import pandas as pd
import os
import argparse

# Configuration: Maps report types to their specific IP column names
# Based on the headers defined in shadowserver_files_processor.py
COLUMN_MAPPING = {
    'vuln': 'IP',          # Destination.csv
    'blocklist': 'ip',     # Blocklist_events.csv
    'sinkhole': 'src_ip'   # Sinkhole_events.csv
}

def split_csv_by_ip_prefix(source_file, output_dir, file_type='vuln'):
    """
    Splits a CSV file into smaller files based on the first 3 octets of the IP address.
    The IP column is determined by the file_type.
    """
    if not os.path.exists(source_file):
        print(f"❌ Error: Source file not found at: {source_file}")
        return

    os.makedirs(output_dir, exist_ok=True)
    
    # Determine the target column
    ip_col = COLUMN_MAPPING.get(file_type, 'IP')
    print(f"ℹ️  Processing mode: '{file_type}'. Target IP column: '{ip_col}'")

    try:
        print(f"🔄 Reading source file...")
        df = pd.read_csv(source_file, dtype=str, low_memory=False)
        
        # Clean column names (strip whitespace)
        df.columns = [col.strip() for col in df.columns]
        
        if ip_col not in df.columns:
            print(f"❌ Error: Column '{ip_col}' not found in CSV.")
            print(f"   Available columns: {list(df.columns)}")
            return
            
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    def get_ip_prefix(ip_val):
        # Handle non-string or malformed IPs
        if not isinstance(ip_val, str) or ip_val.count('.') < 2: 
            return "invalid_ip"
        # Extract first 3 octets (e.g., 192.168.1)
        return ".".join(ip_val.split('.')[:3])

    print("⚙️  Generating IP prefixes...")
    # Create a temporary column for grouping
    df['temp_ip_prefix'] = df[ip_col].apply(get_ip_prefix)
    
    grouped = df.groupby('temp_ip_prefix')
    
    print(f"📊 Found {len(grouped)} unique prefixes. Writing files...")
    
    count = 0
    for prefix, group_df in grouped:
        if prefix == "invalid_ip":
            filename = "records_with_invalid_ips.csv"
        else:
            # Replace dots with underscores for filesystem safety
            filename = f"{prefix.replace('.', '_')}.csv"
            
        output_path = os.path.join(output_dir, filename)
        
        # Drop the temp column before saving
        group_df.drop(columns=['temp_ip_prefix']).to_csv(output_path, index=False)
        # Optional: print(f"   -> Saved {len(group_df)} records to {filename}")
        count += 1

    print(f"\n🎉 Splitting complete! Created {count} files in '{output_dir}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split Shadowserver CSV reports by IP subnet.")
    
    parser.add_argument("source_file", help="Path to the source CSV file.")
    parser.add_argument("output_dir", help="Directory where split files will be saved.")
    
    # New argument to specify the file type
    parser.add_argument(
        "-t", "--type", 
        choices=['vuln', 'blocklist', 'sinkhole'], 
        default='vuln',
        help="The type of report being processed. Determines which column is used for splitting (IP, ip, or src_ip)."
    )
    
    args = parser.parse_args()
    
    split_csv_by_ip_prefix(args.source_file, args.output_dir, args.type)