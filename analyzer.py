import pandas as pd
import argparse
from pathlib import Path

def generate_report(df):
    """
    Analyzes the dataframe and prints a formatted report to the console.
    """
    # --- Data Preparation ---
    # Ensure 'State' column exists and filter for open issues
    if 'State' not in df.columns or df['State'].dropna().empty:
        print("'State' column is missing or empty. Cannot filter for open issues.")
        return

    df_open = df[df['State'].str.strip().str.lower() == 'open'].copy()
    
    if df_open.empty:
        print("✅ No 'open' issues found to analyze.")
        return
        
    # Convert 'Recurring Issue' to a number for calculation
    df_open['Recurring Issue'] = pd.to_numeric(df_open['Recurring Issue'], errors='coerce').fillna(0)

    # --- Report Generation ---
    print("\n" + "="*50)
    print("      Vulnerability Intelligence Report")
    print("="*50)

    # 1. Overall Summary
    print("\n--- 📊 Overall Summary ---")
    print(f"Total Open Issues: {len(df_open)}")
    if 'Severity' in df_open.columns:
        print("\nBreakdown by Severity:")
        severity_counts = df_open['Severity'].str.strip().value_counts()
        print(severity_counts.to_string())
    
    # 2. Top Offenders
    print("\n--- 🎯 Top 5 Most Common Issues ---")
    if 'Issue' in df_open.columns:
        top_issues = df_open['Issue'].str.strip().value_counts().nlargest(5)
        print(top_issues.to_string())
    
    print("\n--- 💻 Top 5 IPs with Most Open Issues ---")
    if 'IP' in df_open.columns:
        top_ips = df_open['IP'].str.strip().value_counts().nlargest(5)
        print(top_ips.to_string())

    # 3. Geographical Distribution
    print("\n--- 🌍 Issues by Region ---")
    if 'Region' in df_open.columns and not df_open['Region'].dropna().empty:
        region_counts = df_open['Region'].str.strip().value_counts()
        print(region_counts.to_string())
    else:
        print("No region data available.")

    # 4. Recurring Problems
    print("\n--- 🔄 Recurring Issues ---")
    recurring_count = len(df_open[df_open['Recurring Issue'] > 0])
    print(f"Total issues that are recurring: {recurring_count}")

    print("\n" + "="*50)


def main():
    """
    Main function to load data and initiate analysis.
    """
    parser = argparse.ArgumentParser(
        description="Analyzes the master vulnerability CSV file and generates an intelligence report."
    )
    parser.add_argument(
        "filepath",
        help="Path to the destination.csv file to be analyzed."
    )
    args = parser.parse_args()
    
    destination_file = Path(args.filepath)
    
    if not destination_file.exists():
        print(f"Error: File not found at '{destination_file}'")
        return

    print(f"Reading data from '{destination_file}'...")
    try:
        df = pd.read_csv(destination_file, dtype=str)
        generate_report(df)
    except Exception as e:
        print(f"Failed to read or process the file. Reason: {e}")

if __name__ == "__main__":
    main()