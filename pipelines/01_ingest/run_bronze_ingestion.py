import duckdb
from datetime import datetime
from pathlib import Path
import pandas as pd

from parsers.extractor_base import extract_sap_raw_data
from parsers import zpost_parser, se16_parser
from pipelines.utils.metadata import get_conn, get_file_hash, is_already_processed, log_processed_file, log_run

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR      = PROJECT_ROOT / "data" / "00_raw"
BRONZE_DIR   = PROJECT_ROOT / "data" / "01_bronze"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# ── Config: Register parsers here ──────────────────────────────────────────
BRONZE_CONFIGS = [
    {
        "parser_module": zpost_parser,
    },
    {
        "parser_module": se16_parser,
    },
    # Add more parsers here - first match wins
]

def process_file(file_path: Path, conn: duckdb.DuckDBPyConnection) -> int:
    """
    Process a single raw file through the parser pipeline.

    - Checks if file already processed (hash-based deduplication)
    - Extracts tabular data using base extractor
    - Matches file to registered parsers
    - Writes Bronze Parquet and logs metadata
    """
    print(f"\nProcessing: {file_path.name}")

    # ── Step 1: Check if already processed ────────────────────────────────
    file_hash = get_file_hash(file_path)
    if is_already_processed(conn, file_hash):
        print(f"  SKIPPED: Already processed (Hash: {file_hash[:8]}...)")
        return 0

    # ── Step 2: Extract raw data (strips page breaks) ─────────────────────
    first_line, headers, data = extract_sap_raw_data(file_path)
    if not data:
        print(f"  SKIPPED: No parsable tabular data found")
        return 0

    # ── Step 3: Match to registered parsers ───────────────────────────────
    selected_parser = None

    for cfg in BRONZE_CONFIGS:
        parser = cfg["parser_module"]
        if parser.is_match(first_line, headers):
            selected_parser = parser
            print(f"  Matched parser: {parser.__name__}")
            break

    # ── Step 4: Parse data ─────────────────────────────────────────────────
    if selected_parser:
        table_name, df = selected_parser.parse(first_line, headers, data, file_path)
    else:
        print(f"  Using generic fallback parser")
        table_name = file_path.stem
        df = pd.DataFrame(data, columns=headers)

    # ── Step 5: Write to Bronze ───────────────────────────────────────────
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"bronze_{table_name.lower()}_{current_date}.parquet"
    output_path = BRONZE_DIR / output_filename

    # Cast all to string for Bronze layer schema safety
    df = df.astype(str)
    conn.execute(f"COPY (SELECT * FROM df) TO '{output_path}' (FORMAT PARQUET, CODEC 'ZSTD');")

    # ── Step 6: Log metadata ───────────────────────────────────────────────
    log_processed_file(conn, file_path.name, file_hash, len(df), output_filename)

    print(f"  SUCCESS: {output_filename}")
    print(f"  Rows: {len(df):,}")

    return len(df)

def main():
    """Process all raw files in the RAW_DIR."""

    conn = get_conn()
    total_rows = 0
    files_processed = 0
    status = "success"

    try:
        raw_files = list(RAW_DIR.glob("*.txt"))

        if not raw_files:
            print(f"\nNo .txt files found in {RAW_DIR}")
            print("Place raw SAP exports in this directory and re-run.")
            return

        print(f"\nFound {len(raw_files)} raw file(s)")

        for file_path in raw_files:
            try:
                rows = process_file(file_path, conn)
                if rows:
                    total_rows += rows
                    files_processed += 1
            except Exception as e:
                print(f"  ERROR: {e}")
                status = "partial"
                continue

    except Exception as e:
        status = "failed"
        print(f"\nFATAL ERROR: {e}")
        raise

    finally:
        log_run(conn, "bronze_ingestion", str(BRONZE_DIR), total_rows, files_processed, status)
        conn.close()

        print(f"Bronze layer processing complete!")
        print(f"Files processed: {files_processed}")
        print(f"Total rows ingested: {total_rows:,}")

if __name__ == "__main__":
    main()
