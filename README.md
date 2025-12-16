# marva-stats

Generate stats from MARVA's backend database BSON files.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

### 1. Extract Data

Place your BSON export files in the `bson_data/` directory.
This should be the resourcesProduction.bson from the backup. 
You can put multiple resourcesProduction.bson there even if their records
overlap that is fine the program will dedupe based on e number. So put the 
backups in that cover all the desired range of time.

```bash
python3 extract_published.py <start_date> <end_date>
```

Date format: `yyyy-mm-dd`

Example:
```bash
python3 extract_published.py 2024-10-01 2025-09-30
```

This script:
- Scans all BSON files in `bson_data/`
- Filters for published records within the date range
- Deduplicates records by URI across files
- Extracts NAR IDs (n20255...) from XML content (needs to modified for 2026+)
- Outputs JSON files to `output/`:
  - `records_by_day.json` - record counts per day
  - `records_by_month.json` - record counts per month
  - `records_by_user_month.json` - record counts by user by month
  - `nars_by_day.json` - new NAR counts per day
  - `nars_by_month.json` - new NAR counts per month
  - `nars_by_user_month.json` - new NAR counts by user by month
  - `nars_list.json` - list of all NAR URLs found
- Saves sample XML files containing NARs to `xml_data/`

### 2. Generate Reports

After extracting data, generate CSV and PNG reports:

```bash
python3 generate_reports.py
```

This creates reports in `reports/`:
- `marva_records_by_day_<dates>.csv` - daily record counts
- `marva_records_by_day_<dates>.png` - histogram of daily records
- `marva_records_by_month_<dates>.csv` - monthly record counts
- `marva_records_by_user_month_<dates>.csv` - records by user by month
- `marva_nars_by_day_<dates>.csv` - daily NAR counts
- `marva_nars_by_day_<dates>.png` - histogram of daily NARs
- `marva_nars_by_month_<dates>.csv` - monthly NAR counts
- `marva_nars_by_user_month_<dates>.csv` - NARs by user by month

All reports include totals and use the date range in filenames.

## Directory Structure

```
marva-stats/
├── bson_data/          # Place BSON export files here
├── output/             # JSON intermediate files
├── reports/            # CSV and PNG reports
├── xml_data/           # Sample XML files (gitignored)
├── extract_published.py
├── generate_reports.py
└── requirements.txt
```
