# 🦆 SAP HR & Finance ETL Pipeline (DuckDB + Python)

> **A high-performance, local ETL pipeline built to ingest unstructured SAP exports, resolve complex overlapping temporal boundaries, and apply Medallion Architecture principles.**

## 📖 The Problem
Enterprise SAP data is notoriously difficult to work with outside the ERP environment. This project tackles two major data engineering challenges:
1. **Unstructured Raw Data:** SAP background jobs export data as `.txt` files polluted with visual page breaks, repeating headers, and inconsistent delimiters. 
2. **Unaligned Temporal Boundaries (SCD Type 2):** HR data (employee roles) and Finance data (cost centers) change on completely different schedules. Joining them to calculate daily costs results in massive data duplication and overlapping `Start_Date` / `End_Date` intervals that standard SQL `JOIN` or `BETWEEN` clauses cannot handle.

## 💡 The Solution
This project solves these issues by implementing a **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** powered entirely by **Python** and **DuckDB** for lightning-fast, local execution:
- **Custom Ingestion Engine (Bronze):** A modular, object-oriented parsing system that dynamically strips SAP page breaks, identifies tabular boundaries, and safely casts mixed-type data to Parquet.
- **Idempotent Execution & State Management:** Uses MD5 file hashing and a local DuckDB metadata database to track processed files, preventing duplicate ingestion and providing a complete audit trail (rows in, rows out, rows filtered).
- **Gaps & Islands Temporal Join (Gold):** Solves the overlapping date problem using an advanced DuckDB window function pattern:
  - **Explode:** Unnests date ranges into individual days using `GENERATE_SERIES`.
  - **Join:** Safely joins HR and Finance data on specific days.
  - **Collapse:** Uses `LAG()` to identify "islands" of unchanged state, grouping the daily data back into clean, newly aligned date intervals while perfectly allocating financial costs.

## 🛠️ Tech Stack & Engineering Practices
* **Data Processing:** DuckDB, SQL, Pandas
* **Architecture:** Medallion (Bronze/Silver/Gold), Gaps and Islands, SCD Type 2
* **Engineering Rigor:** Object-Oriented Python, Config-driven generic runners, Editable Installs (`pyproject.toml`), Environment Variables (`.env`), Idempotency.

---

## Directory Structure
```text
/sap_etl_project/
├── .env                              ← Environment variables (e.g., credentials, file paths)
├── pyproject.toml                    ← Project configuration (setuptools backend)
├── README.md
├── data/                             ← Ignored by git (except for .gitkeep)
│   ├── 00_raw/                       ← Messy SAP .txt exports
│   ├── 01_bronze/                    ← 1-to-1 Parquet copies (Strings only)
│   ├── 02_silver/temporal_snapshots/ ← Cleaned Parquet, typed dates/decimals
│   ├── 03_gold/                      ← Final Temporal Join (Explode/Collapse)
│   ├── 04_reporting/                 ← Excel output files
│   └── metadata/
│       └── etl_metadata.duckdb       ← Persistent state & audit logging
├── notebooks/                        ← Jupyter notebooks for data exploration
├── pipelines/                        ← Core package (installed via pip install -e .)
│   ├── 01_ingest/
│   │   ├── ingest_raw_files.py       ← Main caller, handles state & hashing
│   │   └── parsers/                  ← Modular regex/pipe parsers (e.g. zpost_parser, se16_parser)
│   ├── 02_transform/
│   │   ├── sql/                      ← Templated SQL queries for silver transformations
│   │   └── run_silver_transform.py   ← Generic runner applying SQL templates to Bronze files
│   ├── 03_aggregate/
│   │   ├── build_hr_master.py        ← The core ASOF/Temporal Explode logic
│   │   └── extract_pa_mismatches.py  ← Data quality check (DuckDB to Excel)
│   └── utils/
│       └── metadata.py               ← Helper functions (get_conn, logging)
└── tests/
    └── clean_metadata.py
```

## Setup & Installation

This project is packaged using `pyproject.toml` and requires an editable install to resolve internal module imports (like `from pipelines.utils import ...`) dynamically without path-hacking.

```bash
# Clone the repository
git clone https://github.com/awychoi/SAP-HR-Finance-ETL-Pipeline.git
cd SAP-HR-Finance-ETL-Pipeline

# Create a virtual environment and activate it
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install the project in editable mode
pip install -e .

# Configure Environment Variables
# Create a .env file based on project requirements (e.g., DUCKDB_PATH)
```

## 1. Bronze Ingestion Strategy
- **Format Handling**: Raw `.txt` files use `|` delimiters with repeating header rows caused by SAP page breaks. A modular parser architecture (`extractor_base.py`) was built to bypass page breaks and isolate tabular data.
- **Dynamic Parsing**: `ingest_raw_files.py` loops through raw files and attempts to match them against a list of `REGISTERED_PARSERS`. If no specific parser matches, it falls back to a generic parser.
- **Idempotency**: The pipeline uses file hashing (`get_file_hash()`) and checks against `etl_metadata.duckdb` to prevent re-processing identical files.
- **Schema Safety**: All Bronze data is explicitly cast to `str` before writing to Parquet. This prevents DuckDB schema inference from crashing on mixed-type columns.

## 2. Silver Transformation Strategy
- **Generic Runner & SQL Templates**: Instead of separate Python scripts for each table, `run_silver_transform.py` iterates over a `TRANSFORM_CONFIGS` dictionary. It reads templated `.sql` files from the `sql/` directory, injects file paths and filters dynamically, and executes them.
- **One Read, One Write**: All Silver logic (trimming, casting, filtering) is executed in a single DuckDB SQL `COPY (SELECT ...) TO` query directly from the Bronze parquet file.
- **Audit Logging**: Every Silver script records `input_rows`, `output_rows`, and the number of filtered rows into the metadata DB via `log_transform()`.

## 3. Gold Temporal Join Strategy
Because `ZPOST`, `PA0001`, and `PA0008` have entirely unaligned and overlapping `Start Date` and `End Date` boundaries, standard `BETWEEN` joins fail. This was solved using a 4-step "Explode -> Join -> Collapse" (Gaps and Islands) pattern in DuckDB.

1. **Explode (zpost_daily)**: 
   Use `UNNEST(GENERATE_SERIES(adj_valid_from, adj_valid_to, INTERVAL 1 DAY))` to create one row per day. Divide all 12 monetary columns by `Number of Days` to calculate a daily rate.
2. **Join (joined_daily)**:
   Left join PA0001 and PA0008 where `zpost_daily.daily_date BETWEEN pa.valid_from AND pa.valid_to`.
3. **Identify Islands (identified_changes)**:
   Use the `LAG()` window function over the HR attributes to flag `is_new_period = 1` whenever an attribute changes. Run a cumulative sum over `is_new_period` to generate a `period_group_id`.
4. **Collapse (grouped_islands)**:
   `GROUP BY period_group_id`, take the `MIN()` and `MAX()` of the daily dates to form the new temporal boundaries, and `SUM()` the daily monetary rates to perfectly allocate costs. Use `ANY_VALUE(COLUMNS(* EXCLUDE(...)))` to dynamically carry through static ZPOST columns.

---
Disclosure: Rapid prototype augmented with Perplexity. Medallion pipeline workflow design, folder structure, DuckDB temporal modeling and code review, modification, testing, and finalization by yours truly.
