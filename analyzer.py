import argparse
import sys
from pathlib import Path
import pandas as pd


def get_dynamic_col(df, candidates):
    """
    Finds the first matching column name in the DataFrame from a list of
    candidate names (case-insensitive). Returns None if no match is found.
    """
    lower_cols = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_cols:
            return lower_cols[candidate.lower()]
    return None


def print_top_counts(df, col_name, section_title, top_n=5):
    """
    Safely cleans, filters out empty/null values, and prints the top N
    value counts for a given column.
    """
    if not col_name or col_name not in df.columns:
        return

    # Clean whitespace and filter out blank or NaN entries
    cleaned = df[col_name].dropna().astype(str).str.strip()
    cleaned = cleaned[(cleaned != "") & (cleaned.str.lower() != "nan") & (cleaned.str.lower() != "none")]

    if cleaned.empty:
        return

    print(f"\n--- {section_title} (Column: '{col_name}') ---")
    if top_n:
        print(cleaned.value_counts().nlargest(top_n).to_string())
    else:
        print(cleaned.value_counts().to_string())


def generate_report(df):
    """
    Generates a comprehensive, schema-agnostic intelligence summary report
    from any Shadowserver CSV DataFrame.
    """
    if df is None or df.empty:
        print("[INFO] The provided dataset is empty. Nothing to analyze.")
        return

    # Clean column header whitespace
    df.columns = [str(col).strip() for col in df.columns]

    # Dynamically identify key columns across all 145+ Shadowserver report schemas
    state_col = get_dynamic_col(df, ['state'])
    rec_col = get_dynamic_col(df, ['recurring issue', 'recurring_issue'])
    sev_col = get_dynamic_col(df, ['severity', 'level'])
    issue_col = get_dynamic_col(df, ['issue', 'type', 'tag'])
    infection_col = get_dynamic_col(df, ['infection', 'family'])
    ip_col = get_dynamic_col(df, ['src_ip', 'ip', 'dst_ip', 'source_ip'])
    port_col = get_dynamic_col(df, ['src_port', 'port', 'dst_port'])
    proto_col = get_dynamic_col(df, ['protocol', 'proto'])
    region_col = get_dynamic_col(df, ['src_region', 'region', 'dst_region', 'src_geo', 'geo'])
    asn_col = get_dynamic_col(df, ['src_asn', 'asn', 'dst_asn'])

    # Filter for 'open' records if a State column exists; otherwise analyze all rows
    if state_col:
        df_active = df[df[state_col].astype(str).str.strip().str.lower() == 'open'].copy()
        if df_active.empty:
            print("[INFO] No 'open' issues found in the dataset.")
            return
        scope_label = "Total Open Issues"
    else:
        print("[INFO] 'state' column not found. Analyzing all records in file.")
        df_active = df.copy()
        scope_label = "Total Records Analyzed"

    # Calculate recurring issue count if the tracking column is present
    recurring_total = 0
    if rec_col:
        df_active[rec_col] = pd.to_numeric(df_active[rec_col], errors='coerce').fillna(0)
        recurring_total = int((df_active[rec_col] > 0).sum())

    # --- Print Formatted Report ---
    print("\n" + "=" * 55)
    print("       SHADOWSERVER INTELLIGENCE SUMMARY REPORT")
    print("=" * 55)

    print("\n--- Overall Summary ---")
    print(f"{scope_label}: {len(df_active)}")
    if rec_col:
        print(f"Recurring Issues:  {recurring_total}")

    # Breakdown by Severity (all severities)
    print_top_counts(df_active, sev_col, "Breakdown by Severity", top_n=None)

    # Top Issues / Categories
    print_top_counts(df_active, issue_col, "Top 5 Most Common Issues", top_n=5)

    # Top Malware Infections / Families (common in sinkhole, botnet, and special feeds)
    if infection_col and infection_col != issue_col:
        print_top_counts(df_active, infection_col, "Top 5 Infections / Families", top_n=5)

    # Top Affected IPs (supports IP, src_ip, dst_ip)
    print_top_counts(df_active, ip_col, "Top 5 IPs with Most Events", top_n=5)

    # Top Targeted/Source Ports and Protocols
    print_top_counts(df_active, port_col, "Top 5 Ports", top_n=5)
    print_top_counts(df_active, proto_col, "Breakdown by Protocol", top_n=None)

    # Geographic and Network (ASN) Distribution
    print_top_counts(df_active, region_col, "Top 10 Regions / Geos", top_n=10)
    print_top_counts(df_active, asn_col, "Top 5 ASNs", top_n=5)

    print("\n" + "=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dynamically analyze any processed Shadowserver CSV report."
    )
    parser.add_argument(
        "filepath",
        help="Path to the CSV file to analyze (e.g., dst/accessible_rdp_events.csv)"
    )
    args = parser.parse_args()

    target_path = Path(args.filepath)
    if not target_path.exists():
        print(f"[ERROR] File not found: {target_path}")
        sys.exit(1)

    try:
        df_input = pd.read_csv(target_path, dtype=str, low_memory=False)
        generate_report(df_input)
    except Exception as e:
        print(f"[ERROR] Failed to analyze '{target_path}': {e}")
        sys.exit(1)