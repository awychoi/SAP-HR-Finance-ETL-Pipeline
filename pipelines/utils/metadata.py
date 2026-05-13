import hashlib
import duckdb
from pathlib import Path

METADATA_DB = Path(__file__).resolve().parent.parent.parent / "data" / "metadata" / "etl_metadata.duckdb"
METADATA_DB.parent.mkdir(parents=True, exist_ok=True)


def get_conn() -> duckdb.DuckDBPyConnection:
    """Returns a connection to the shared metadata DB, initialising all tables if needed."""
    conn = duckdb.connect(str(METADATA_DB))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            filename     VARCHAR,
            file_hash    VARCHAR,
            processed_at TIMESTAMP DEFAULT current_timestamp,
            row_count    INTEGER,
            output_file  VARCHAR,
            PRIMARY KEY (file_hash)
        )
    """)
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS pipeline_runs_seq START 1
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          INTEGER PRIMARY KEY DEFAULT nextval('pipeline_runs_seq'),
            script_name     VARCHAR,
            output_dir      VARCHAR,
            total_rows      INTEGER,
            files_processed INTEGER,
            status          VARCHAR,
            run_at          TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transform_log (
            table_name       VARCHAR,
            layer            VARCHAR,
            source_file      VARCHAR,
            output_file      VARCHAR,
            input_row_count  INTEGER,
            output_row_count INTEGER,
            rows_filtered    INTEGER,
            processed_at     TIMESTAMP DEFAULT current_timestamp
        )
    """)

    return conn


# ── Ingest helpers ──────────────────────────────────────────────────────────

def get_file_hash(file_path: Path) -> str:
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def is_already_processed(conn: duckdb.DuckDBPyConnection, file_hash: str) -> bool:
    result = conn.execute(
        "SELECT COUNT(*) FROM processed_files WHERE file_hash = ?", [file_hash]
    )
    assert result is not None
    row = result.fetchone()
    assert row is not None
    return row[0] > 0


def log_processed_file(conn: duckdb.DuckDBPyConnection, filename: str,
                       file_hash: str, row_count: int, output_file: str):
    conn.execute("""
        INSERT INTO processed_files (filename, file_hash, row_count, output_file)
        VALUES (?, ?, ?, ?)
    """, [filename, file_hash, row_count, output_file])


# ── Run-level helper (all layers) ───────────────────────────────────────────

def log_run(conn: duckdb.DuckDBPyConnection, script_name: str, output_dir: str,
            total_rows: int, files_processed: int, status: str):
    conn.execute("""
        INSERT INTO pipeline_runs (script_name, output_dir, total_rows, files_processed, status)
        VALUES (?, ?, ?, ?, ?)
    """, [script_name, output_dir, total_rows, files_processed, status])


# ── Transform audit helper (silver / gold) ──────────────────────────────────

def log_transform(conn: duckdb.DuckDBPyConnection, table_name: str, layer: str,
                  source_file: str, output_file: str,
                  input_row_count: int, output_row_count: int):
    conn.execute("""
        INSERT INTO transform_log
            (table_name, layer, source_file, output_file,
             input_row_count, output_row_count, rows_filtered)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [table_name, layer, source_file, output_file,
          input_row_count, output_row_count, input_row_count - output_row_count])
