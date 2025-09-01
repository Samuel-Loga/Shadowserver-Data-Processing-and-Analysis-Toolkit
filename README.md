## Table of Contents

- [Features](#features)  
- [Scripts Overview](#scripts-overview)  
- [Prerequisites](#prerequisites)  
- [Recommended File Structure](#recommended-file-structure)  
- [Script Usage](#script-usage)  
  - [1. shadowserver_files_processor.py - Data Aggregator](#1-shadowserver_files_processorpy---data-aggregator)  
  - [2. deduplicate_records.py - Record Cleaner](#2-deduplicate_recordspy---record-cleaner)  
  - [3. split_csv_by_ip.py - Data Splitter](#3-split_csv_by_ippy---data-splitter)  
  - [4. analyzer.py - Intelligence Reporter](#4-analyzerpy---intelligence-reporter)  
- [Recommended Workflow](#recommended-workflow)  


# Vulnerability Data Processing and Analysis Toolkit

This toolkit provides a suite of Python scripts designed to automate the processing, cleaning, analysis, and management of vulnerability scan reports, particularly those from sources like Shadowserver. It allows you to aggregate raw data, de-duplicate records, split large datasets, and generate summary intelligence reports.

---

## Features

- **Automated Processing:** Ingest new CSV or Excel reports into a central master file, automatically tracking processed files.  
- **Data De-duplication:** Consolidate redundant records based on key fields, keeping the most recent entry and updating a Recurring Issue counter.  
- **Data Segmentation:** Split the master CSV file into smaller, more manageable files based on IP address prefixes (e.g., /24 subnets).  
- **Intelligence Reporting:** Generate a text-based summary of the vulnerability data, including breakdowns by severity, top offenders, and geographical region.  

---

## Scripts Overview

- **`shadowserver_files_processor.py`**: The primary data aggregation script. It reads new reports from a source directory and merges them into a master `destination.csv`.  
- **`deduplicate_records.py`**: A cleaning utility that processes the master file to remove redundant entries while intelligently updating the recurring issue count.  
- **`split_csv_by_ip.py`**: A segmentation script that splits a large CSV into multiple smaller files based on the first three octets of the IP address.  
- **`analyzer.py`**: A reporting tool that reads a processed CSV file and prints a summary analysis to the console.  

---

## Prerequisites

Before using these scripts, ensure you have Python and the pandas library installed.

- **Python 3:** Ensure you have Python 3.6 or newer installed.  
- **Pandas:** Install the library using pip:

```bash
pip install pandas
```

## Recommended File Structure
For the scripts to work seamlessly, it's recommended to organize your directories as follows:

shadow_intel_processor/
|-- dst/
|   |-- destination.csv             # Master aggregated file from the processor script
|   |-- destination_deduplicated.csv  # (Optional) Cleaned master file
|   `-- processed_files.txt         # Log of processed source files
|-- splitted/
|   |-- 192_168_1.csv               # Example split file
|   `-- ...                         # Other split files
|-- src/
|   |-- report_1.csv                # New raw report files go here
|   `-- report_2.xlsx
|
|-- shadowserver_files_processor.py
|-- deduplicate_records.py
|-- split_csv_by_ip.py
|-- analyzer.py
`-- README.md

## Script Usage
### 1. `shadowserver_files_processor.py` - Data Aggregator
This script is your starting point. It reads all .csv and .xlsx files from the src directory, processes them, and appends the data to the master destination.csv file in the dst directory. It keeps track of processed files in processed_files.txt to prevent re-processing.

**Modes:**

- **process (Default):** Reads, processes, and saves new data  
- **discover:** Dry run to identify new files and count records without making changes

**How to Use:**

```bash
# To process new files (default mode)
python shadowserver_files_processor.py process

# To run a dry run and see what would be processed
python shadowserver_files_processor.py discover
```

### 2. `deduplicate_records.py` - Record Cleaner
This script reads a source CSV (like destination.csv), removes redundant records, and saves a cleaned version. A record is considered redundant if it has the same Severity, IP, Protocol, Port, and State as another record. The script keeps the most recent entry and updates its Recurring Issue column with the count of the duplicates it removed.

**How to Use:**

python deduplicate_records.py <source_file> <output_file>

Example:

```bash
python deduplicate_records.py "d:\PD\shadow_intel_processor\dst\destination.csv" "d:\PD\shadow_intel_processor\dst\destination_deduplicated.csv"
```

### 3. `split_csv_by_ip.py` - Data Splitter
Use this script to break down a large CSV file into smaller ones, where each new file contains records sharing the same first three IP octets (e.g., 192.168.1.x). This is useful for assigning network blocks to different teams.

**How to Use:**

python split_csv_by_ip.py <source_file> <output_dir>

Example:

```bash
python split_csv_by_ip.py "d:\PD\shadow_intel_processor\dst\destination_deduplicated.csv" "d:\PD\shadow_intel_processor\splitted"
```

This will create files like 192_168_1.csv inside the splitted directory.

### 4. `analyzer.py` - Intelligence Reporter
This script generates a high-level summary of the data in a given CSV file. It provides counts of open issues, breakdowns by severity, top 5 most common issues, top 5 IPs with the most issues, and more.

**How to Use:**

python analyzer.py <filepath>

Example:

```bash
python analyzer.py "d:\PD\shadow_intel_processor\dst\destination_deduplicated.csv"
```

## Recommended Workflow
A typical workflow for using these scripts would be:

### 1. Aggregate 
Place new raw report files into the src/ directory. Run the processor to merge them into your master file.

```bash
python shadowserver_files_processor.py
```

### 2. De-duplicate
Run the de-duplication script on the master file to create a clean, consolidated view.

```bash
python deduplicate_records.py "dst\destination.csv" "dst\destination_deduplicated.csv"
```

### 3. Analyze or Split
To get a high-level overview of your entire network, run the analyzer on the de-duplicated file.

```bash
python analyzer.py "dst\destination_deduplicated.csv"
```

To divide the work, run the splitter script. You can then run the analyzer on any of the smaller, split files.

```bash
python split_csv_by_ip.py "dst\destination_deduplicated.csv" "splitted"
```