# 🦆 Modular SAP ETL Pipeline (DuckDB + Python)

> **A high-performance, local ETL pipeline built to ingest unstructured SAP exports, resolve complex overlapping temporal boundaries, and apply Medallion Architecture principles.**

## 📖 The Problem
Enterprise SAP data is notoriously difficult to work with outside the ERP environment. This project tackles two major data engineering challenges:
1. **Polluted Exports:** SAP background jobs export data as `.txt` files polluted with visual page breaks, repeating headers, and inconsistent delimiters. 
2. **Unaligned Temporal Boundaries (SCD Type 2):** Many SAP tables behave as independently changing effective-dated datasets, with validity windows that overlap but do not align cleanly across domains. Resolving those intervals for accurate point-in-time analysis requires temporal segmentation logic beyond standard SQL joins.

## 💡 The Solution
This project solves these issues by pairing an object-oriented Python extraction framework with DuckDB's advanced temporal modeling capabilities. The pipeline organizes these transformations through a strict Medallion Architecture (Bronze ➔ Silver ➔ Gold) to guarantee data quality, idempotency, and lightning-fast local execution:
- **Custom Ingestion Engine (Bronze):** A modular, object-oriented parsing system that dynamically strips SAP page breaks, identifies table name and tabular boundaries, and safely casts mixed-type data to Parquet.
- **Idempotent Execution & State Management:** Uses MD5 file hashing and a local DuckDB metadata database to track processed files, preventing duplicate ingestion and providing a complete audit trail (rows in, rows out, rows filtered).
- **Gaps & Islands Temporal Join (Gold):** Solves the overlapping date problem using an advanced DuckDB window function pattern that leverages optimized range joins (20-30× faster than traditional approaches).

## 🛠️ Tech Stack & Engineering Practices
* **Data Processing:** DuckDB, SQL, Pandas
* **Architecture:** Medallion (Bronze/Silver/Gold), Gaps and Islands, SCD Type 2
* **Engineering Rigor:** Object-Oriented Python, Config-driven generic runners, Editable Installs (`pyproject.toml`), Environment Variables (`.env`), Idempotency.

---

## 📋 Prerequisites

Before starting, ensure the following are installed:
- **Python 3.9+** (tested on 3.9, 3.10, 3.11)
- **pip** (latest)

**Hardware Recommendations:**
- Minimum 4GB RAM (8GB+ recommended for large SAP exports)

---

## ⚠️ Important Considerations

This pipeline is designed as a **framework**, not a plug-and-play solution. Because SAP environments are highly customized, you must adapt the provided code to match your organization's specific business rules, master data configurations, and regional requirements.

Intermediate python and SQL knowledge is required. Knowledge of data engineering concepts and DuckDB is a plus.

---

## 🚀 Setup & Installation

### 1. Clone the Repository
```bash
git clone https://github.com/awychoi/Modular-SAP-ETL-Pipeline.git
cd Modular-SAP-ETL-Pipeline
```

### 2. Create Virtual Environment
```bash
# Create a virtual environment
python -m venv venv

# Activate it
source venv/bin/activate  # On Windows: venv\\Scripts\\activate
```

### 3. Install the Project
```bash
# Install in editable mode (allows dynamic imports)
pip install -e .
```

### 4. Configure Environment Variables
Create a `.env` file in the project root directory:

```bash
# .env file - Copy and customize these values

# Database Configuration
DUCKDB_PATH=data/metadata/etl_metadata.duckdb

# Data Directory Paths
RAW_DATA_PATH=data/00_raw
BRONZE_PATH=data/01_bronze
SILVER_PATH=data/02_silver/temporal_snapshots
GOLD_PATH=data/03_gold
REPORTING_PATH=data/04_reporting

# Optional: Logging Level
LOG_LEVEL=INFO
```

**Environment Variable Reference:**

| Variable | Purpose | Default Value | Example |
|----------|---------|---------------|---------|
| `DUCKDB_PATH` | Location of metadata database | `data/metadata/etl_metadata.duckdb` | `data/metadata/etl_metadata.duckdb` |
| `RAW_DATA_PATH` | Directory for raw SAP `.txt` files | `data/00_raw` | `data/00_raw` |
| `BRONZE_PATH` | Directory for Bronze Parquet files | `data/01_bronze` | `data/01_bronze` |
| `SILVER_PATH` | Directory for cleaned Silver files | `data/02_silver/temporal_snapshots` | `data/02_silver/temporal_snapshots` |
| `GOLD_PATH` | Directory for final joined tables | `data/03_gold` | `data/03_gold` |
| `REPORTING_PATH` | Directory for Excel output files | `data/04_reporting` | `data/04_reporting` |
| `LOG_LEVEL` | Logging verbosity | `INFO` | `DEBUG`, `INFO`, `WARNING` |

### 5. Verify Installation
```bash
# Check that the package is installed
pip list | grep pipelines

# Verify directory structure
ls -la data/
```

---

## 📂 Directory Structure
```text
/SAP-HR-Finance-ETL-Pipeline/
├── .env                              ← Environment variables
├── pyproject.toml                    ← Project configuration
├── README.md
├── data/                             ← Ignored by git (except for .gitkeep)
│   ├── 00_raw/                       ← Raw SAP .txt exports
│   ├── 01_bronze/                    ← bronze_*.parquet (strings only)
│   ├── 02_silver/temporal_snapshots/ ← silver_*.parquet (typed, cleaned)
│   ├── 03_gold/                      ← gold_*.parquet (final aggregations)
│   ├── 04_reporting/                 ← Various output files
│   └── metadata/
│       └── etl_metadata.duckdb       ← Persistent state & audit logging
├── notebooks/                        ← Jupyter notebooks for exploration
├── pipelines/                        ← Core package (installed via pip install -e .)
│   ├── 01_ingest/
│   │   ├── run_bronze_ingestion.py   ← Bronze layer runner
│   │   └── parsers/                  ← Modular parsers (zpost_parser, se16_parser)
│   ├── 02_transform/
│   │   ├── sql/                      ← SQL templates for Silver transformations
│   │   └── run_silver_transform.py   ← Silver layer runner
│   ├── 03_aggregate/
│   │   ├── sql/                      ← SQL templates for Gold aggregations
│   │   └── run_gold_aggregations.py  ← Gold layer runner
│   ├── 04_reporting/
│   │   └── extract_pa_mismatches.py  ← report extractor
│   └── utils/
│       └── metadata.py               ← Helper functions (get_conn, logging)
└── tests/
    └── clean_metadata.py
```

---

## 🎯 Quick Start Guide

### Running the Complete Pipeline

Execute the pipeline in three sequential steps:

#### **Step 1: Bronze Ingestion**
Place your raw SAP `.txt` files in `data/00_raw/`, then run:

```bash
python pipelines/01_ingest/run_bronze_ingestion.py
```

**What this does:**
- Scans `data/00_raw/` for new or modified `.txt` files
- Matches files to registered parsers
- Strips page breaks and extracts tabular data
- Writes Parquet files to `data/01_bronze/` with naming: `bronze_tablename_timestamp.parquet`
- Logs metadata to `etl_metadata.duckdb`

**Expected output:**
```
Found 3 raw file(s)

Processing: ZPOST_export.txt
  Matched parser: zpost_parser
  Description: ZPOST financial posting data
  SUCCESS: bronze_zpost_20260514_120830.parquet
  Rows: 12,450

======================================================================
Bronze layer processing complete!
Files processed: 3
Total rows ingested: 21,373
======================================================================
```

#### **Step 2: Silver Transformation**
Clean and type-cast Bronze data:

```bash
python pipelines/02_transform/run_silver_transform.py
```

**What this does:**
- Reads SQL templates from `pipelines/02_transform/sql/`
- Applies transformations (trimming, casting dates/decimals)
- Filters invalid records
- Writes typed Parquet to `data/02_silver/` with naming: `silver_tablename_timestamp.parquet`
- Logs input/output row counts

**Expected output:**
```

Starting Silver transformation: zpost
  Description: ZPOST financial posting data
  Source: bronze_zpost_20260514_120830.parquet
  SUCCESS: silver_zpost_20260514_120845.parquet
  Rows: 12,450 → 12,444 (6 filtered)

```

#### **Step 3: Gold Aggregation**
Build the temporal join using range-based logic:

```bash
python pipelines/03_aggregate/run_gold_aggregations.py
```

**What this does:**
- Performs temporal join with overlapping date ranges
- Allocates costs across time periods
- Writes final table to `data/03_gold/` with naming: `gold_tablename_timestamp.parquet`

**Expected output:**
```
Starting Gold aggregation: hr_financial_summary
  Description: Temporal join with overlapping range logic
  Dependency [zpost]: silver_zpost_20260514_120845.parquet
  Dependency [pa1]: silver_pa0001_20260514_120846.parquet
  Dependency [pa8]: silver_pa0008_20260514_120847.parquet
  SUCCESS: gold_hr_financial_summary_20260514_121000.parquet
  Rows: 12,444 → 8,932

```

---

## ⚙️ Customization Guide

This pipeline is designed to be highly customizable through a **unified config-driven architecture** across all three layers.

### Unified Pattern Across All Layers

All three layers (Bronze, Silver, Gold) follow the same design pattern:
1. **Config list** at the top of the runner file
2. **Add entries** to customize behavior
3. **Run the script** - no code changes needed

```
┌─────────────────────────────────────────────────────┐
│ BRONZE_CONFIGS = [                                  │
│     {"parser_module": ...}                          │
│ ]                                                   │
└─────────────────────────────────────────────────────┘
          ↓
┌─────────────────────────────────────────────────────┐
│ SILVER_CONFIGS = [                                  │
│     {"output_name": ..., "sql_file": ...,           │
│      "dependencies": {...}, "filters": ...,         | 
|      "description": ...}                            |
│ ]                                                   │
└─────────────────────────────────────────────────────┘
          ↓
┌─────────────────────────────────────────────────────┐
│ GOLD_CONFIGS = [                                    │
│     {"output_name": ..., "sql_file": ...,           │
│      "dependencies": {...}, "description": ...}     │
│ ]                                                   │
└─────────────────────────────────────────────────────┘
```

---

### Adding a New Custom Parser (Bronze Layer)

The pipeline uses a **modular parser architecture**. Parser files define detection and extraction logic; the main ingestion script handles orchestration, file hashing, and metadata logging.

#### **Parser Structure**

Create a new file in `pipelines/01_ingest/parsers/` with two required functions:

```python
import re
import pandas as pd

def is_match(first_line: str, headers: list) -> bool:
    """Return True if this parser should handle the file."""
    return "YOUR_UNIQUE_IDENTIFIER" in first_line

def parse(first_line: str, headers: list, data: list, file_path) -> tuple:
    """Extract table name and return DataFrame.

    Args:
        first_line: First non-empty line from raw file
        headers: Column headers (already extracted)
        data: Row data (page breaks already removed)
        file_path: Path object of source file

    Returns:
        (table_name, DataFrame)
    """
    # Extract table name from pattern or use filename as fallback
    match = re.search(r'YourPattern\\s+([A-Z0-9_]+)', first_line)
    table_name = match.group(1) if match else file_path.stem

    df = pd.DataFrame(data, columns=headers)
    return table_name, df
```

**Key Points:**
- `is_match()`: Detection logic only - return `True`/`False`
- `parse()`: Must return `(table_name, DataFrame)` tuple
- Page breaks and raw file parsing are handled upstream by `extract_sap_raw_data()`
- File I/O or database writes are not in parsers

#### **Registration**

Add parser to `BRONZE_CONFIGS` in `run_bronze_ingestion.py`:

```python
from parsers import zpost_parser, se16_parser, your_parser

BRONZE_CONFIGS = [
    {
        "parser_module": zpost_parser,
    },
    {
        "parser_module": se16_parser,
    },
    {
        "parser_module": your_parser,
    },
]
```

#### **Testing**

```bash
# Place test file in data/00_raw/
python pipelines/01_ingest/ingest_raw_files.py

# Verify output
ls data/01_bronze/
```

If no parser matches, the pipeline automatically uses a generic fallback (filename as table name).

---

### Modifying Silver Transformations

The Silver layer uses a **config-driven architecture**. SQL templates define transformations; a generic runner applies them to Bronze files.

#### **SQL Template Structure**

Create a new file in `pipelines/02_transform/sql/` (e.g., `your_table.sql`):

```sql
SELECT
    -- Type casting and cleaning
    CASE
        WHEN TRIM("Your Date Column") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("Your Date Column"), '%d.%m.%Y')::DATE
    END AS your_date_field,

    -- Column renaming and trimming
    TRIM("Original Name") AS new_name,
    NULLIF(TRIM("Nullable Field"), '') AS nullable_field,

    -- Keep remaining columns (exclude transformed ones)
    * EXCLUDE ("Your Date Column", "Original Name", "Nullable Field")
FROM '{source_file}'
{filters}
```

**Template Variables:**
- `{source_file}`: Automatically injected Bronze file path
- `{filters}`: Optional WHERE clause from config

#### **Configuration**

Add an entry to `SILVER_CONFIGS` in `run_silver_transform.py`:

```python
SILVER_CONFIGS = [
    {
        "output_name":  "your_table",
        "sql_file":     "your_table.sql",
        "dependencies": {
            "source_file": "bronze_your_table_*.parquet",
        },
        "filters":      "WHERE TRIM(\"Key Field\") != ''",
        "description":  "Description of your table",
    },
]
```

**Config Parameters:**
| Parameter | Purpose | Example |
|-----------|---------|---------|
| `output_name` | Name for output file | `"pa0001"` |
| `sql_file` | SQL template filename | `"pa0001.sql"` |
| `dependencies` | Bronze file pattern | `{"source_file": "bronze_pa0001_*.parquet"}` |
| `filters` | Optional WHERE clause | `"WHERE field != ''"` |
| `description` | Self-documenting context | `"Organizational assignment data"` |

#### **Testing**

```bash
# Run all transformations
python pipelines/02_transform/run_silver_transform.py

# Verify output
ls data/02_silver/temporal_snapshots/
```

---

### Customizing the Temporal Join Logic (Gold Layer)

The example Gold layer demonstrates **DuckDB's advanced range join capabilities** for temporal analytics. SAP tables frequently rely on Slowly Changing Dimensions (SCD Type 2) using `VALID_FROM` and `VALID_TO` dates. This implementation solves the complex problem of joining independent SAP tables that have overlapping, unaligned date intervals.

#### **The Pattern: Interval-Based Range Joins**

The core technique uses **overlapping range conditions** instead of exact point-in-time date matching:

```sql
-- Key pattern: Join on overlapping validity intervals
LEFT JOIN sap_dimension_table dim 
    ON base.entity_id = dim.entity_id
    AND base.valid_from <= dim.valid_to    -- Ranges overlap
    AND base.valid_to >= dim.valid_from
```

This leverages DuckDB's optimized range join algorithm, which executes **20-30× faster** than traditional explode-join-collapse patterns used in legacy databases.

#### **What to Customize**

**1. Join Keys and Conditions**
Create a new SQL template in `pipelines/03_aggregate/sql/` to define how your specific SAP modules link together (e.g., joining Transactional Data to Master Data):

```sql
LEFT JOIN '{your_dimension_table}' dim
    ON fact.your_key_field = dim.matching_key
    AND fact.valid_from <= dim.valid_to
    AND fact.valid_to >= dim.valid_from
```

**2. Date Boundaries (Analysis Window)**
Adjust the global analysis window to match your reporting period (e.g., filtering out historical legacy data):

```sql
WHERE adj_valid_from >= DATE '2024-01-01'
  AND adj_valid_to <= DATE '2025-12-31'
```

**3. Time-Weighted Allocations (Metrics)**
When intervals overlap, you often need to prorate business metrics (like budgets, target quantities, or costs) across the exact number of overlapping days:

```sql
-- Prorate a metric across the precise overlap duration
CAST("Metric_Value" AS DECIMAL(18,2)) / NULLIF(CAST("Overlap_Days" AS INTEGER), 0) AS daily_allocated_value

-- Or retain the flat value for snapshot reporting
CAST("Metric_Value" AS DECIMAL(18,2)) AS total_metric_value
```

#### **Configuration**

Add an entry to `GOLD_CONFIGS` in `run_gold_aggregations.py`:

```python
GOLD_CONFIGS = [
    {
        "output_name":  "your_temporal_aggregation",
        "sql_file":     "your_aggregation.sql",
        "dependencies": {
            "fact_table": "silver_table1_*.parquet",
            "dim_table":  "silver_table2_*.parquet",
        },
        "description": "Temporal join of SAP module X and Y",
    },
]
```

#### **Testing**

```bash
# Run all aggregations
python pipelines/03_aggregate/run_gold_aggregations.py

# Verify output
ls data/03_gold/
```

#### **Common Customization Scenarios**

| Scenario | Modification |
|----------|--------------|
| **Add another SCD2 Dimension** | Add another `LEFT JOIN` with the same overlap range conditions. |
| **Custom Fiscal Year** | Modify the boundary date filters in the SQL template to match your SAP Fiscal Year Variant. |
| **Period-Based Allocation** | Change the divisor to standard monthly periods (e.g., 30) or use `EXTRACT(DAY FROM ...)`. |
| **Filter Active Records Only** | Add a `WHERE status_flag = 'Active'` or filter out SAP deletion indicators. |
| **Business User Export** | Pass the final Gold Parquet file to the `04_reporting` layer to generate `.xlsx` files. |

---


## 🐛 Troubleshooting

### Common Issues and Solutions

#### **Issue: `ModuleNotFoundError: No module named 'pipelines'`**
**Cause:** The package was not installed in editable mode.  
**Solution:**
```bash
pip install -e .
```

#### **Issue: Parser not matching files**
**Cause:** The `is_match()` method isn't recognizing your file pattern.  
**Solution:** Check the parser's detection logic and add debug logging.

#### **Issue: `DuckDB I/O Error: Cannot open file`**
**Cause:** File paths in `.env` don't exist or have incorrect permissions.  
**Solution:**
```bash
# Create missing directories
mkdir -p data/00_raw data/01_bronze data/02_silver/temporal_snapshots data/03_gold data/04_reporting data/metadata

# Check permissions
chmod -R 755 data/
```

#### **Issue: Memory error during Gold aggregation**
**Cause:** Date range explosion creates too many rows.  
**Solution:** Reduce the date range in your SQL template:
```sql
WHERE adj_valid_from >= DATE '2025-01-01'
  AND adj_valid_to <= DATE '2025-12-31'
```

#### **Issue: Character encoding errors**
**Cause:** SAP exports may use non-UTF-8 encoding.  
**Solution:** Modify parser to handle different encodings:
```python
with open(file_path, 'r', encoding='latin-1') as f:  # or 'cp1252'
    lines = f.readlines()
```

---

## 📊 Monitoring & Audit Logs

The pipeline maintains comprehensive audit logs in `data/metadata/etl_metadata.duckdb`.

### Viewing Ingestion Logs

```python
import duckdb

conn = duckdb.connect('data/metadata/etl_metadata.duckdb')

# View recent ingestions
print(conn.execute('''
    SELECT 
        file_name,
        rows_ingested,
        status,
        ingestion_timestamp
    FROM ingestion_log
    ORDER BY ingestion_timestamp DESC
    LIMIT 10
''').df())
```

### Viewing Transformation Logs

```python
# View transformation statistics
print(conn.execute('''
    SELECT 
        table_name,
        input_rows,
        output_rows,
        filtered_rows,
        transform_timestamp
    FROM transformation_log
    ORDER BY transform_timestamp DESC
    LIMIT 10
''').df())
```

---

## 🤝 Contributing

Contributions are welcome! If you'd like to improve this pipeline:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-parser`)
3. Make your changes and add tests
4. Commit your changes (`git commit -m 'Add amazing new parser'`)
5. Push to the branch (`git push origin feature/amazing-parser`)
6. Open a Pull Request

---

## 📄 License

This project is available under the MIT License. See LICENSE file for details.

---

## 🙏 Acknowledgments

Disclosure: Rapid prototype augmented with Perplexity. Medallion pipeline workflow design, folder structure, DuckDB temporal modeling and code review, modification, testing, and finalization by yours truly.

**Key Techniques & Patterns Demonstrated:**
- Medallion Architecture (Bronze / Silver / Gold / Publish)
- Config-Driven ETL Pipeline Design
- DuckDB Overlapping Range Joins for Temporal Data
- Slowly Changing Dimensions (SCD Type 2) Resolution
- Idempotent Execution & Hash-Based Deduplication
- Semi-Structured Data Parsing (Custom Python Extractors)
- Decoupled Storage & Presentation (Reverse ETL Pattern)
