from pathlib import Path
from datetime import datetime
from pipelines.utils.metadata import get_conn, log_transform

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR   = PROJECT_ROOT / "data" / "01_bronze"
SILVER_DIR   = PROJECT_ROOT / "data" / "02_silver" / "temporal_snapshots"
SQL_DIR      = Path(__file__).resolve().parent / "sql"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ── Config: one entry per table, declare what's different ──────────────────
TRANSFORM_CONFIGS = [
    {
        "table_name":    "PA0001",
        "bronze_glob":   "PA0001_*.parquet",
        "output_prefix": "base_pa0001",
        "sql_file":      "pa0001.sql",
        "filters":       "WHERE TRIM(\"Pers.No.\") != '' AND TRIM(\"Position\") NOT IN ('', '99999999') AND TRIM(\"Cost Ctr\") != 'DUMMY'",
    },
    {
        "table_name":    "PA0008",
        "bronze_glob":   "PA0008_*.parquet",
        "output_prefix": "base_pa0008",
        "sql_file":      "pa0008.sql",
        "filters":       "",
    },
    {
        "table_name":    "ZPOST",
        "bronze_glob":   "ZPOST_*.parquet",
        "output_prefix": "base_zpost",
        "sql_file":      "zpost.sql",
        "filters":       "",
    },
]

# ── Generic runner — same boilerplate for every table ─────────────────────
def run_transform(cfg: dict):
    table   = cfg["table_name"]
    print(f"Starting Silver transformation for {table}...")

    bronze_files = sorted(BRONZE_DIR.glob(cfg["bronze_glob"]))
    if not bronze_files:
        print(f"  No {table} files found in Bronze layer.")
        return

    source_file = bronze_files[-1]
    current_date    = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{cfg['output_prefix']}_{current_date}.parquet"
    output_path     = SILVER_DIR / output_filename

    sql_template = (SQL_DIR / cfg["sql_file"]).read_text().rstrip().rstrip(";")

    sql_rendered = sql_template.format(
        source_file = source_file.resolve().as_posix(),
        filters     = cfg.get("filters", "")
    )

    query = f"""
        COPY (
            {sql_rendered}
        ) TO '{output_path.resolve().as_posix()}' (FORMAT PARQUET, CODEC 'ZSTD');
    """

    conn = get_conn()
    input_rows  = conn.execute(f"SELECT COUNT(*) FROM '{source_file.resolve().as_posix()}'").fetchone()[0]
    conn.execute(query)
    output_rows = conn.execute(f"SELECT COUNT(*) FROM '{output_path.resolve().as_posix()}'").fetchone()[0]

    log_transform(conn, table, "silver", source_file.name, output_filename, input_rows, output_rows)
    conn.close()

    print(f"  -> Success! {output_filename} ({output_rows} rows, {input_rows - output_rows} filtered)")


def main():
    for cfg in TRANSFORM_CONFIGS:
        run_transform(cfg)


if __name__ == "__main__":
    main()