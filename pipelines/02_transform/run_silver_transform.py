from pathlib import Path
from datetime import datetime
from pipelines.utils.metadata import get_conn, log_transform, log_run

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR   = PROJECT_ROOT / "data" / "01_bronze"
SILVER_DIR   = PROJECT_ROOT / "data" / "02_silver" / "temporal_snapshots"
SQL_DIR      = Path(__file__).resolve().parent / "sql"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ── Config: Add new transformations by adding entries here ─────────────────
SILVER_CONFIGS = [
    {
        "output_name":  "pa0001",
        "sql_file":     "pa0001.sql",
        "dependencies": {
            "source_file": "*_pa0001_*.parquet",
        },
        "filters":      "WHERE TRIM(\"Pers.No.\") != '' AND TRIM(\"Position\") NOT IN ('', '99999999') AND TRIM(\"Cost Ctr\") != 'DUMMY'",
        "description":  "pa0001 organizational assignment data",
    },
    {
        "output_name":  "pa0008",
        "sql_file":     "pa0008.sql",
        "dependencies": {
            "source_file": "*_pa0008_*.parquet",
        },
        "filters":      "",
        "description":  "pa0008 basic pay data",
    },
    {
        "output_name":  "zpost",
        "sql_file":     "zpost.sql",
        "dependencies": {
            "source_file": "ZPOST_*.parquet",
        },
        "filters":      "",
        "description":  "zpost financial posting data",
    },
    # Add more Silver transformations here following the same pattern
]


def run_silver_transform(cfg: dict):
    """
    Generic runner for Silver transformations.

    - Finds latest Bronze file
    - Renders SQL template with source file and filters
    - Executes transformation and logs metadata
    """
    output_name = cfg["output_name"]
    print(f"\nStarting Silver transformation: {output_name}")
    print(f"  Description: {cfg['description']}")

    # ── Step 1: Resolve dependencies (find latest Bronze file) ────────────
    file_paths = {}
    missing_deps = []

    for alias, pattern in cfg["dependencies"].items():
        bronze_files = sorted(BRONZE_DIR.glob(pattern))

        if not bronze_files:
            missing_deps.append(f"{alias} ({pattern})")
            continue

        file_paths[alias] = bronze_files[-1].resolve().as_posix()
        print(f"  Source: {bronze_files[-1].name}")

    if missing_deps:
        print(f"  ERROR: Missing dependencies: {', '.join(missing_deps)}")
        return

    # ── Step 2: Prepare output path ────────────────────────────────────────
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"silver_{output_name}_{current_date}.parquet"
    output_path = SILVER_DIR / output_filename

    # ── Step 3: Render SQL template ────────────────────────────────────────
    sql_template_path = SQL_DIR / cfg["sql_file"]

    if not sql_template_path.exists():
        print(f"  ERROR: SQL template not found: {sql_template_path}")
        return

    sql_template = sql_template_path.read_text().rstrip().rstrip(";")

    # Inject both file paths and filters
    sql_rendered = sql_template.format(
        **file_paths,
        filters=cfg.get("filters", "")
    )

    # ── Step 4: Execute transformation ─────────────────────────────────────
    conn = get_conn()
    status = "success"
    final_rows = 0

    try:
        # Capture input count
        primary_file = list(file_paths.values())[0]
        input_rows = conn.execute(f"SELECT COUNT(*) FROM '{primary_file}'").fetchone()[0]

        # Execute the full query
        full_query = f"""
            COPY (
                {sql_rendered}
            ) TO '{output_path.resolve().as_posix()}' (FORMAT PARQUET, CODEC 'ZSTD');
        """

        conn.execute(full_query)
        final_rows = conn.execute(f"SELECT COUNT(*) FROM '{output_path.resolve().as_posix()}'").fetchone()[0]

        # ── Step 5: Log metadata ───────────────────────────────────────────
        source_files_str = " + ".join([Path(p).name for p in file_paths.values()])

        log_transform(
            conn,
            table_name       = output_name,
            layer            = 'silver',
            source_file      = source_files_str,
            output_file      = output_filename,
            input_row_count  = input_rows,
            output_row_count = final_rows
        )

        filtered_rows = input_rows - final_rows
        print(f"  SUCCESS: {output_filename}")
        print(f"  Rows: {input_rows:,} → {final_rows:,} ({filtered_rows:,} filtered)")

    except Exception as e:
        status = "failed"
        print(f"  ERROR: {e}")
        raise

    finally:
        log_run(conn, f"silver_{output_name}", str(SILVER_DIR), final_rows, 1, status)
        conn.close()


def main():
    """Run all configured Silver transformations."""

    for cfg in SILVER_CONFIGS:
        try:
            run_silver_transform(cfg)
        except Exception as e:
            print(f"  Failed to process {cfg['output_name']}: {e}")
            continue

if __name__ == "__main__":
    main()
