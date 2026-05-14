import duckdb
from pathlib import Path
from datetime import datetime
from pipelines.utils.metadata import get_conn, log_transform, log_run


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR   = PROJECT_ROOT / "data" / "02_silver" / "temporal_snapshots"
GOLD_DIR     = PROJECT_ROOT / "data" / "03_gold"
SQL_DIR      = Path(__file__).resolve().parent / "sql"

GOLD_DIR.mkdir(parents=True, exist_ok=True)


# ── Config: Add new aggregations by adding entries here ────────────────────
GOLD_CONFIGS = [
    {
        "output_name":  "hr_summary",
        "sql_file":     "temporal_join.sql",
        "dependencies": {
            "zpost": "base_zpost_*.parquet",
            "pa1":   "base_pa0001_*.parquet",
            "pa8":   "base_pa0008_*.parquet",
        },
        "description": "Temporal join with overlapping range logic",
    },
    # Add more Gold aggregations here following the same pattern:
    # {
    #     "output_name":  "monthly_cost_summary",
    #     "sql_file":     "monthly_summary.sql",
    #     "dependencies": {
    #         "base": "hr_financial_summary_*.parquet",  # Can reference other Gold tables
    #     },
    #     "description": "Monthly rollup by department",
    # },
]


def run_gold_aggregation(cfg: dict):
    """
    Generic runner for Gold aggregations.

    - Finds latest files for each dependency
    - Renders SQL template with file paths
    - Executes query and logs metadata
    """
    output_name = cfg["output_name"]
    print(f"\nStarting Gold aggregation: {output_name}")
    print(f"  Description: {cfg['description']}")

    # ── Step 1: Resolve dependencies (find latest file for each) ──────────
    file_paths = {}
    missing_deps = []

    for alias, pattern in cfg["dependencies"].items():
        # Check both Silver and Gold directories for dependencies
        silver_files = sorted(SILVER_DIR.glob(pattern))
        gold_files = sorted(GOLD_DIR.glob(pattern))
        all_files = silver_files + gold_files

        if not all_files:
            missing_deps.append(f"{alias} ({pattern})")
            continue

        file_paths[alias] = all_files[-1].resolve().as_posix()
        print(f"  Dependency [{alias}]: {all_files[-1].name}")

    if missing_deps:
        print(f"  ERROR: Missing dependencies: {', '.join(missing_deps)}")
        return

    # ── Step 2: Prepare output path ────────────────────────────────────────
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{output_name}_{current_date}.parquet"
    output_path = GOLD_DIR / output_filename

    # ── Step 3: Render SQL template ────────────────────────────────────────
    sql_template_path = SQL_DIR / cfg["sql_file"]

    if not sql_template_path.exists():
        print(f"  ERROR: SQL template not found: {sql_template_path}")
        return

    sql_template = sql_template_path.read_text()
    sql_rendered = sql_template.format(**file_paths)

    # ── Step 4: Execute query ──────────────────────────────────────────────
    query_conn = duckdb.connect()
    meta_conn = get_conn()

    status = "success"
    final_rows = 0

    try:
        # Capture input count from primary dependency (first one)
        primary_file = list(file_paths.values())[0]
        input_rows = query_conn.execute(f"SELECT COUNT(*) FROM '{primary_file}'").fetchone()[0]

        # Execute the full query
        full_query = f"""
            COPY (
                {sql_rendered}
            ) TO '{output_path.resolve().as_posix()}' (FORMAT PARQUET, CODEC 'ZSTD');
        """

        query_conn.execute(full_query)
        final_rows = query_conn.execute(f"SELECT COUNT(*) FROM '{output_path.resolve().as_posix()}'").fetchone()[0]

        # ── Step 5: Log metadata ───────────────────────────────────────────
        source_files_str = " + ".join([Path(p).name for p in file_paths.values()])

        log_transform(
            meta_conn,
            table_name       = output_name,
            layer            = 'gold',
            source_file      = source_files_str,
            output_file      = output_filename,
            input_row_count  = input_rows,
            output_row_count = final_rows
        )

        print(f"  SUCCESS: {output_filename}")
        print(f"  Rows: {input_rows:,} → {final_rows:,}")

    except Exception as e:
        status = "failed"
        print(f"  ERROR: {e}")
        raise

    finally:
        log_run(meta_conn, f"gold_{output_name}", str(GOLD_DIR), final_rows, 1, status)
        query_conn.close()
        meta_conn.close()


def main():
    """Run all configured Gold aggregations."""
    for cfg in GOLD_CONFIGS:
        try:
            run_gold_aggregation(cfg)
        except Exception as e:
            print(f"  Failed to process {cfg['output_name']}: {e}")
            continue

if __name__ == "__main__":
    main()
