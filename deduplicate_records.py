import pandas as pd
import os
import argparse

def deduplicate_and_update(source_file, output_file):
    """
    De-duplicates records based on a set of key columns, keeping the latest entry
    and updating the 'Recurring Issue' count.
    """
    # --- 1. Load the Data ---
    try:
        print(f"Loading data from '{source_file}'...")
        df = pd.read_csv(source_file, dtype=str, low_memory=False)
        print(f"Found {len(df)} total records.")
    except FileNotFoundError:
        print(f"Error: Source file not found at '{source_file}'")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # --- 2. Prepare the Data for Processing ---
    # Convert Timestamp to a datetime object to allow for correct sorting.
    # Errors will be converted to 'NaT' (Not a Time), which can be handled.
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    # Convert 'Recurring Issue' to a numeric type for calculations.
    # Invalid parsing will be set as 0.
    df['Recurring Issue'] = pd.to_numeric(df['Recurring Issue'], errors='coerce').fillna(0).astype(int)

    # Define the columns that identify a unique, recurring issue.
    key_columns = ['Severity', 'IP', 'Protocol', 'Port', 'State']

    # Drop rows where key identifiers or the timestamp are missing.
    df.dropna(subset=key_columns + ['Timestamp'], inplace=True)

    # --- 3. Sort and Identify Duplicates ---
    # Sort by the key columns and then by Timestamp in descending order.
    # This places the most recent record for each group at the top.
    print("Sorting records to find the latest entries...")
    df_sorted = df.sort_values(by=key_columns + ['Timestamp'], ascending=[True]*len(key_columns) + [False])

    # Group by the key columns to find duplicates.
    grouped = df_sorted.groupby(key_columns)
    
    final_records = []
    
    print(f"Processing {len(grouped)} unique issue groups...")
    
    for _, group in grouped:
        # The first row in each group is the latest one, which we will keep.
        latest_record = group.iloc[0].copy()
        
        # The number of duplicates is the size of the group minus the one we're keeping.
        num_duplicates = len(group) - 1
        
        if num_duplicates > 0:
            # Add the count of removed duplicates to the existing recurring count.
            latest_record['Recurring Issue'] += num_duplicates
            
        final_records.append(latest_record)

    # --- 4. Create and Save the Final DataFrame ---
    if not final_records:
        print("No valid records found to process.")
        return
        
    df_final = pd.DataFrame(final_records)
    
    # Sort the final result by timestamp for a clean, chronological view.
    df_final = df_final.sort_values(by='Timestamp', ascending=False)
    
    try:
        df_final.to_csv(output_file, index=False)
        print("\n--- Success! ---")
        print(f"Removed {len(df) - len(df_final)} redundant records.")
        print(f"Saved {len(df_final)} unique records to '{output_file}'")
    except Exception as e:
        print(f"Error saving the final CSV file: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="De-duplicate vulnerability records in a CSV file, keeping the latest entry and updating the recurring issue count."
    )
    parser.add_argument("source_file", help="The path to the source CSV file with redundant records.")
    parser.add_argument("output_file", help="The path where the cleaned, de-duplicated CSV file will be saved.")
    
    args = parser.parse_args()
    
    deduplicate_and_update(args.source_file, args.output_file)