# Shadowserver Data Processing and Analysis Toolkit

This suite of Python scripts are designed to automate the processing, cleaning, analysis, and management of vulnerability scan reports, particularly those from sources like **Shadowserver**. It allows you to aggregate raw data, de-duplicate records, split large datasets, and generate summary intelligence reports.

## Table of Contents

- [Features](#features)  
- [Scripts Overview](#scripts-overview)  
- [Prerequisites](#prerequisites)  
- [Recommended File Structure](#recommended-file-structure)  
- [Script Usage](#script-usage)
  - [1. email_downloader.py - Email Dowanloader](#1-email_downloaderpy---email-dowanloader) 
  - [2. shadowserver_files_processor.py - Data Aggregator](#2-shadowserver_files_processorpy---data-aggregator)  
  - [3. deduplicate_records.py - Record Cleaner](#3-deduplicate_recordspy---record-cleaner)  
  - [4. split_csv_by_ip.py - Data Splitter](#4-split_csv_by_ippy---data-splitter)  
  - [5. analyzer.py - Intelligence Reporter](#5-analyzerpy---intelligence-reporter)

---

## Features

- **Automated Processing:** Ingest new CSV or Excel reports into a central master file, automatically tracking processed files.  
- **Data De-duplication:** Consolidate redundant records based on key fields, keeping the most recent entry and updating a Recurring Issue counter.  
- **Data Segmentation:** Split the master CSV file into smaller, more manageable files based on IP address prefixes (e.g., /24 subnets).  
- **Intelligence Reporting:** Generate a text-based summary of the vulnerability data, including breakdowns by severity, top offenders, and geographical region.  

---

## Scripts Overview

- **`email_downloader.py`**: Automates fetching of reports from Shadowserver emails. It connects to your email inbox via IMAP, searches for unread emails from Shadowserver, and downloads report files to `src` folder.
- **`shadowserver_files_processor.py`**: The primary data aggregation script. It reads new reports from a source directory and merges them into a master `destination.csv`.  
- **`deduplicate_records.py`**: A cleaning utility that processes the master file to remove redundant entries while intelligently updating the recurring issue count.  
- **`split_csv_by_ip.py`**: A segmentation script that splits a large CSV into multiple smaller files based on the first three octets of the IP address.  
- **`analyzer.py`**: A reporting tool that reads a processed CSV file and prints a summary analysis to the console.  

---

## Prerequisites

Before using these scripts, ensure you have Python, the pandas, requests and python-dotenv libraries installed.

- **Python 3:** Ensure you have Python 3.6 or newer installed.  
- **Python Libraries:** Install the libraries using pip:

```bash
pip install pandas requests python-dotenv
```

- **Email Configuration** *(Important for download of email files)*
To keep your credentials secure, this script uses environment variables stored in a `.env` file. It does not hardcode your password.

Create a new file named .env in the same directory as email_downloader.py. Add the following lines to the file:

```bash
SHADOWSERVER_EMAIL_USER=your_email@gmail.com
SHADOWSERVER_EMAIL_PASS=your_16_char_app_password
```

***Note:*** 

For `Google Users`, you may proceed as is. For `Outlook/Exchange Users`, you may need to change the IMAP_SERVER variable in `email_downloader.py` to `outlook.office365.com`.

## Recommended File Structure
For the scripts to work seamlessly, it's recommended to organize your directories as follows:

```bash
shadow_intel_processor/
├── dst/
│ ├── destination.csv # Master aggregated file
│ ├── destination_deduplicated.csv # (Optional) Cleaned master file
│ └── processed_files.txt # Log of processed source files
├── splitted/
│ ├── 192_168_1.csv # Example split file
│ └── ... # Other split files
├── src/
│ ├── report_1.csv # New raw report files go here
│ └── report_2.xlsx
├── .env
├── shadowserver_files_processor.py
├── deduplicate_records.py
├── email_downloader.py
├── split_csv_by_ip.py
├── analyzer.py
└── README.md

```
## Script Usage
A typical recommended workflow for using these scripts would be:

### 1. `email_downloader.py` - Email Dowanloader
This script is your starting point. It automates the process of fetching vulnerability reports from Shadowserver emails. It connects to your email inbox via IMAP, searches for unread emails from Shadowserver, and intelligently downloads report files. 

**Configuration Options:**

You can modify these settings directly in email_downloader.py if needed:

- `DOWNLOAD_FOLDER`: Default is d:\PD\shadow_intel_processor\src. Change this if you want files saved elsewhere.

- `SEARCH_CRITERIA`: Update sending profile '(`UNSEEN FROM` "sending-profile@shadowserver.org")'. `UNSEEN` only checks emails you haven't opened yet. Remove `UNSEEN` to check all emails (useful for the first run).

- `IMAP_SERVER`: Default is "imap.gmail.com". Change this to match your email provider (e.g., imap.mail.yahoo.com).

**How to Use:**

You can run the script directly from your terminal to fetch emails immediately.

```bash
python email_downloader.py
```

If your `.env` file is set up correctly, it will start automatically. If not, it will prompt you to enter your email and password manually.

### 2. `shadowserver_files_processor.py` - Data Aggregator
It reads all .csv files from the src directory, processes them, and appends the data to the master destination.csv file in the dst directory. It keeps track of processed files in processed_files.txt to prevent re-processing.

**How to Use:**

Place new raw report files (.csv) into the src/ directory. Run the processor to merge them into your master file.

```bash
# To read, process, merge and save new data files
python shadowserver_files_processor.py
```

### 3. `deduplicate_records.py` - Record Cleaner
This script reads a source CSV (like destination.csv), removes redundant records, and saves a cleaned version. A record is considered redundant if it has the same Severity, IP, Protocol, Port, and State as another record. The script keeps the most recent entry and updates its Recurring Issue column with the count of the duplicates it removed.

**How to Use:**
Run the de-duplication script on the master file to create a clean, consolidated view.

python deduplicate_records.py <source_file> <output_file>

Example:

```bash
python deduplicate_records.py "dst\destination.csv" "dst\destination_deduplicated.csv"
```

### 4. `split_csv_by_ip.py` - Data Splitter
Use this script to break down a large CSV file into smaller ones, where each new file contains records sharing the same first three IP octets (e.g., 192.168.1.x). This is useful for assigning network blocks to different teams.

**How to Use:**

python split_csv_by_ip.py <source_file> <output_dir>


Example:

```bash
python split_csv_by_ip.py "dst\destination_deduplicated.csv" "splitted"
```

This will create files like 192_168_1.csv inside the splitted directory.

### 5. `analyzer.py` - Intelligence Reporter
This script generates a high-level summary of the data in a given CSV file. It provides counts of open issues, breakdowns by severity, top 5 most common issues, top 5 IPs with the most issues, and more.

**How to Use:**

python analyzer.py <filepath>

Example:

```bash
python analyzer.py "dst\destination_deduplicated.csv"
```
