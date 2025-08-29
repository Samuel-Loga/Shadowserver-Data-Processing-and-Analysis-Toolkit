import pandas as pd
import os
import argparse

def clean_column_names(df):
    """
    Cleans up DataFrame column names by stripping whitespace and removing duplicates.
    """
    # The user's provided column string suggests there might be duplicates or messy names.
    # This function makes the script more robust.
    
    # Step 1: Strip whitespace from each column name
    df.columns = [col.strip() for col in df.columns]
    
    # Step 2: Handle potential duplicate columns by keeping the first occurrence
    seen_columns = {}
    new_columns = []
    for col in df.columns:
        if col not in seen_columns:
            seen_columns[col] = 1
            new_columns.append(col)
        else:
            # If column is a duplicate, append a suffix
            new_columns.append(f"{col}_{seen_columns[col]}")
            seen_columns[col] += 1
            
    df.columns = new_columns
    return df

def split_csv_by_ip_prefix(source_file, output_dir):
    """
    Reads a CSV, groups rows by the first three octets of the 'IP' column,
    and saves each group to a separate CSV file.
    """
    # --- 1. Argument Validation ---
    if not os.path.exists(source_file):
        print(f"Error: Source file not found at '{source_file}'")
        return

    os.makedirs(output_dir, exist_ok=True)
    print(f"Output will be saved to: {output_dir}")

    # --- 2. Read and Prepare the Data ---
    try:
        print(f"Reading source file: {source_file}...")
        df = pd.read_csv(source_file, dtype=str, low_memory=False)
        
        # Clean up column names to handle potential formatting issues
        df = clean_column_names(df)

        # Check if the 'IP' column exists
        if 'IP' not in df.columns:
            print(f"Error: 'IP' column not found in the source file.")
            print(f"   Available columns are: {list(df.columns)}")
            return
            
        print("✔️ Source file loaded successfully.")
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # --- 3. Create the IP Prefix for Grouping ---
    # This function extracts the first three octets (e.g., '192.168.1') from an IP address.
    # It safely handles invalid or empty IP entries.
    def get_ip_prefix(ip):
        if not isinstance(ip, str) or ip.count('.') < 2:
            return "invalid_ip"
        return ".".join(ip.split('.')[:3])

    print("Generating IP prefixes for grouping...")
    df['ip_prefix'] = df['IP'].apply(get_ip_prefix)

    # --- 4. Group and Save Files ---
    # Group the DataFrame by the newly created 'ip_prefix' column
    grouped = df.groupby('ip_prefix')
    
    print(f"Found {len(grouped)} unique IP prefixes. Writing files...")
    
    for prefix, group_df in grouped:
        if prefix == "invalid_ip":
            filename = "records_with_invalid_ips.csv"
        else:
            # Sanitize the prefix for use as a filename
            safe_filename = prefix.replace('.', '_')
            filename = f"{safe_filename}.csv"
        
        output_path = os.path.join(output_dir, filename)
        
        # Drop the temporary 'ip_prefix' column before saving
        group_df_to_save = group_df.drop(columns=['ip_prefix'])
        
        group_df_to_save.to_csv(output_path, index=False)
        print(f"   -> Saved {len(group_df_to_save)} records to {filename}")

    print("\nSplitting process complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Split a large CSV file into smaller files based on the first three octets of an IP address column."
    )
    parser.add_argument("source_file", help="The path to the large, merged CSV file.")
    parser.add_argument("output_dir", help="The path to the directory where the split CSV files will be saved.")
    
    args = parser.parse_args()
    
    split_csv_by_ip_prefix(args.source_file, args.output_dir)