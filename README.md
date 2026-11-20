# Shadowserver Report Toolkit

This Python toolkit helps cybersecurity teams and network administrators manage daily Shadowserver security reports. It automatically downloads reports from your email, cleans and sorts the data, removes duplicates, and flags repeat issues. Everything works on Windows, Mac, and Linux.

---

## What It Does

* **Downloads Reports:** Connects to your email to download new Shadowserver CSV files and zip links.
* **Sorts All Report Types:** Automatically reads any Shadowserver report (over 145 types) and saves each type into its own clean CSV file.
* **Tracks Repeat Issues:** Checks past records to count how many times the same problem has happened before.
* **Removes Duplicates:** Keeps only the newest record for each issue so your lists stay clean.
* **Splits by Network (IP):** Breaks large CSV files into smaller files grouped by the first three numbers of the IP address (for example, `192_168_1.csv`).
* **Creates Summaries:** Prints a quick report showing open issues, severity levels, top affected IPs, and common ports.
* **Builds a JSON Catalog:** Scans the `reports/` folder and creates a `catalog.json` index with dates and row counts.
* **Creates Folders Automatically:** You do not need to create folders manually—the scripts create `src/`, `dst/`, `splitted/`, and `reports/` on their own when needed.

---

## Project Files

```text
shadow_intel_processor/
├── shadow_intel_cli.py               # Main command-line menu to run everything
├── gui_app.py                        # Desktop window app with buttons and AI help
├── email_downloader.py               # Downloads reports from your email
├── shadowserver_files_processor.py   # Cleans, sorts, and merges new CSV files
├── deduplicate_records.py            # Removes duplicate rows
├── split_csv_by_ip.py                # Splits a CSV file by IP subnet
├── analyzer.py                       # Prints a summary of your data
├── generate_catalog.py               # Builds reports/catalog.json
├── .env                              # Stores your email username and password
├── src/                              # Raw downloaded files go here
├── dst/                              # Cleaned and merged files go here
├── splitted/                         # Files split by IP go here
└── reports/                          # Archived reports and catalog.json go here
```

---

## Setup

### 1. Install Required Packages
Make sure you have **Python 3.8 or newer**, then run:

```bash
pip install pandas requests python-dotenv openpyxl
```
*(On Mac or Linux, you may need to use `pip3` and `python3`.)*

### 2. Add Your Email Login
Create a `.env` file in the main project folder and add your email details (for Gmail, use an App Password):

```env
SHADOWSERVER_EMAIL_USER=your_email@example.com
SHADOWSERVER_EMAIL_PASS=your_app_password
```

---

## How to Run

### Option 1: Use the Main Menu (Easiest)
Run the main CLI tool to open a simple numbered menu:

```bash
python shadow_intel_cli.py
```

You can also run tasks directly from the terminal:

* **Download new emails:**
  ```bash
  python shadow_intel_cli.py download
  ```
* **Check what new files are in `src/` without changing anything:**
  ```bash
  python shadow_intel_cli.py process --mode discover
  ```
* **Process and merge all new files:**
  ```bash
  python shadow_intel_cli.py process
  ```
* **Remove duplicates from a file:**
  ```bash
  python shadow_intel_cli.py dedup -i "dst/accessible_rdp_events.csv" -o "dst/clean.csv"
  ```
* **Split a file by IP subnet:**
  ```bash
  python shadow_intel_cli.py split -i "dst/clean.csv" -o "splitted/"
  ```
  * **Split a specific file by IP subnet:**
  \`\`\`bash
  python shadow_intel_cli.py split -i "dst/clean.csv" -o "splitted/"
  \`\`\`
* **Split all CSV files in `dst/` by IP subnet:**
  \`\`\`bash
  python shadow_intel_cli.py split -i all -o "splitted/"
  \`\`\`
* **View a summary report:**
  ```bash
  python shadow_intel_cli.py analyze -f "dst/clean.csv"
  ```

### Option 2: Use the Desktop App
If you prefer clicking buttons instead of typing commands, open the desktop app:

```bash
python gui_app.py
```

### Option 3: Generate the JSON Catalog
To update `reports/catalog.json` from the CSV files inside `reports/`:

```bash
python generate_catalog.py
```