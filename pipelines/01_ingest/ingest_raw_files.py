import duckdb
from datetime import datetime
from pathlib import Path
import pandas as pd

from parsers.extractor_base import extract_sap_raw_data
from parsers import zpost_parser, se16_parser
from pipelines.utils.metadata import get_conn, get_file_hash, is_already_processed, log_processed_file, log_run

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR    = PROJECT_ROOT / "data" / "00_raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "01_bronze"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)

REGISTERED_PARSERS = [zpost_parser, se16_parser]

def process_file(file_path: Path, conn: duckdb.DuckDBPyConnection) -> int:
    print(f"Processing: {file_path.name}...")

    file_hash = get_file_hash(file_path)
    if is_already_processed(conn, file_hash):
        print(f"  -> Skipped: File already processed (Hash: {file_hash[:8]}...)")
        return 0

    first_line, headers, data = extract_sap_raw_data(file_path)
    if not data:
        print(f"  -> Skipped: No parsable tabular data.")
        return 0

    selected_parser = None
    for parser in REGISTERED_PARSERS:
        if parser.is_match(first_line, headers):
            selected_parser = parser
            break

    if selected_parser:
        table_name, df = selected_parser.parse(first_line, headers, data, file_path)
    else:
        print("  -> No specific parser matched. Using generic parser.")
        table_name = file_path.stem
        df = pd.DataFrame(data, columns=headers)

    current_date    = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{table_name}_{current_date}.parquet"
    output_path     = BRONZE_DIR / output_filename

    df = df.astype(str)
    conn.execute(f"COPY (SELECT * FROM df) TO '{output_path}' (FORMAT PARQUET, CODEC 'ZSTD');")

    log_processed_file(conn, file_path.name, file_hash, len(df), output_filename)
    print(f"  -> Success! Saved to {output_filename} ({len(df)} rows)")
    return len(df)


def main():
    conn            = get_conn()
    total_rows      = 0
    files_processed = 0
    status          = "success"

    try:
        for file_path in RAW_DIR.glob("*.txt"):
            try:
                rows = process_file(file_path, conn)
                if rows:
                    total_rows      += rows
                    files_processed += 1
            except Exception as e:
                print(f"  -> Error processing {file_path.name}: {e}")
                status = "partial"

    except Exception as e:
        status = "failed"
        print(f"Fatal pipeline error: {e}")
        raise

    finally:
        log_run(conn, "ingest_sap_source", str(BRONZE_DIR), total_rows, files_processed, status)
        conn.close()


if __name__ == "__main__":
    main()