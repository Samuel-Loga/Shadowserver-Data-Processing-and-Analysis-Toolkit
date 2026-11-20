import os
import re
import json
import datetime
from collections import defaultdict

# --- Configuration ---
REPORTS_DIR = "reports"
CATALOG_FILE = os.path.join(REPORTS_DIR, "catalog.json")

def count_data_rows(filepath):
    """
    Counts the number of data rows in a CSV file (excluding the header).
    Handles reading errors gracefully.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # Count non-empty lines
            lines = sum(1 for line in f if line.strip())
            # Subtract 1 to account for the header row
            return max(0, lines - 1)
    except Exception as e:
        print(f"[WARNING] Could not read file to count rows: {filepath}. Error: {e}")
        return 0

def extract_metadata_from_filename(filename):
    """
    Extracts the Date and the Report Type from the filename.
    Returns a tuple: (date_string, report_type) or (None, None) if invalid.
    """
    # 1. Extract the Date (YYYY-MM-DD)
    date_match = re.match(r'^(\d{4}-\d{2}-\d{2})', filename)
    if not date_match:
        return None, None
    date_str = date_match.group(1)

    # 2. Extract the Report Type (Name)
    report_type = "unknown"
    
    # Primary rule: text appearing between '-scan_' and '-105'
    scan_match = re.search(r'-scan_(.*?)-105', filename)
    
    if scan_match:
        report_type = scan_match.group(1)
    else:
        # Fallback 1: Match the structure from the JSON example (e.g., 2026-07-10-accessible_rdp-malawi...)
        fallback_match = re.match(r'^\d{4}-\d{2}-\d{2}-(.*?)-(?:malawi|105|asn)', filename)
        if fallback_match:
            report_type = fallback_match.group(1)
        else:
            # Fallback 2: Grab the text segment right after the date
            parts = filename.split('-')
            if len(parts) > 3:
                report_type = parts[3].replace('scan_', '').replace('.csv', '')

    return date_str, report_type

def generate_catalog():
    """
    Main function to scan the directory, process CSVs, and create the JSON catalog.
    """
    print(f"[INFO] Scanning directory: {REPORTS_DIR}/ ...")
    
    # Automatically create the reports directory if it does not exist
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR, exist_ok=True)
        print(f"[INFO] Created missing directory '{REPORTS_DIR}/'. Add CSV files and re-run to populate the catalog.")
        return

    # Dictionary to hold the grouped records
    catalog_data = defaultdict(list)
    processed_count = 0

    # Iterate through all files in the reports directory
    for filename in os.listdir(REPORTS_DIR):
        if not filename.lower().endswith('.csv'):
            continue  # Skip non-CSV files

        filepath = os.path.join(REPORTS_DIR, filename)

        # Extract info
        date_str, report_type = extract_metadata_from_filename(filename)
        
        # Graceful handling of unmatched patterns
        if not date_str:
            print(f"[WARNING] Skipping '{filename}': Does not start with a valid YYYY-MM-DD date.")
            continue

        # Get Timestamp from File Metadata (Modification time)
        # Formats to "YYYY-MM-DD HH:MM:SS" as per the example JSON
        mtime = os.path.getmtime(filepath)
        timestamp = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')

        # Count CSV Rows
        rows = count_data_rows(filepath)

        # Generate ID matching the example mock structure: mock-uuid-YYYYMMDD-type
        date_compact = date_str.replace("-", "")
        record_id = f"mock-uuid-{date_compact}-{report_type}"

        # Build the record object
        record = {
            "id": record_id,
            "file": filename,
            "type": report_type,
            "timestamp": timestamp,
            "rows": rows
        }

        # Append to our grouping dictionary
        catalog_data[date_str].append(record)
        processed_count += 1

    if processed_count == 0:
        print("[INFO] No valid CSV files were processed. Catalog not updated.")
        return

    # Sort the catalog by the date keys to ensure chronological order
    sorted_catalog = {k: catalog_data[k] for k in sorted(catalog_data.keys())}

    # Write the output to catalog.json
    try:
        with open(CATALOG_FILE, 'w', encoding='utf-8') as json_file:
            json.dump(sorted_catalog, json_file, indent=2)
        print(f"[SUCCESS] Successfully cataloged {processed_count} files.")
        print(f"[SUCCESS] Catalog saved to: {CATALOG_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to save catalog JSON: {e}")

if __name__ == "__main__":
    generate_catalog()