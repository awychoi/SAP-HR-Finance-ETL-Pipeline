import duckdb
from pathlib import Path
from datetime import datetime

from pipelines.utils.metadata import get_conn, log_transform, log_run

# ==========================================
# Configuration & Paths
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR   = PROJECT_ROOT / "data" / "02_silver" / "temporal_snapshots"
GOLD_DIR     = PROJECT_ROOT / "data" / "03_gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)


def process_temporal_join():
    print("Starting Temporal Join for ZPOST, PA0001, and PA0008...")

    zpost_files = sorted(SILVER_DIR.glob("base_zpost_*.parquet"))
    pa1_files   = sorted(SILVER_DIR.glob("base_pa0001_*.parquet"))
    pa8_files   = sorted(SILVER_DIR.glob("base_pa0008_*.parquet"))

    if not (zpost_files and pa1_files and pa8_files):
        print("Error: Missing required Silver tables for the join.")
        return

    file_zpost = zpost_files[-1]
    file_pa1   = pa1_files[-1]
    file_pa8   = pa8_files[-1]

    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path  = GOLD_DIR / f"gold_hr_financial_summary_{current_date}.parquet"

    query_conn = duckdb.connect()
    meta_conn  = get_conn()

    # Capture input count before the heavy query runs
    input_rows = query_conn.execute(f"SELECT COUNT(*) FROM '{file_zpost}'").fetchone()[0]

    final_rows = 0
    status     = "success"

    try:
        temporal_query = f"""
            COPY (
                WITH

                zpost_clean AS (
                    SELECT
                        TRIM("Personnel Number")     AS employee_id,
                        "Position"                   AS position_id,
                        "Position Pay Group"   AS zpost_group,
                        valid_from,
                        valid_to,

                        CAST("Standard Cost"                  AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_std_cost,
                        CAST("Transfer to Reserves"      AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_other,
                        CAST("Transfer to Fund"    AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_fund,
                        CAST("Transfer Reserve"        AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_charge,
                        CAST("Total Post Cost"                AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_tot_post,
                        CAST("Other Costs"       AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_cother,
                        CAST("Other Charges"                AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_ccharges,
                        CAST("Local Cost"      AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_local_host,
                        CAST("Local Cost1"            AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_local1,
                        CAST("Local Cost2 (Optional)" AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_local2,
                        CAST("Total Non Post Cost"            AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_tot_non_post,
                        CAST("Total"                          AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_total,

                        * EXCLUDE (
                            "Personnel Number", "Position", "Position Pay Group",
                            "Standard Cost", "Transfer to Reserves", "Transfer to Fund",
                            "Transfer Reserve", "Total Post Cost", "Other Costs",
                            "Other Charges", "Local Cost", "Local Cost1",
                            "Local Cost2 (Optional)", "Total Non Post Cost", "Total",
                            "valid_from", "valid_to"
                        )
                    FROM '{file_zpost}'
                    WHERE valid_from IS NOT NULL AND valid_to IS NOT NULL
                ),

                interval_joined AS (
                    SELECT
                        z.employee_id,
                        z.position_id,
                        z.zpost_group,

                        CASE
                            WHEN z.employee_id = '' OR z.employee_id IS NULL THEN NULL
                            ELSE COALESCE(pa8."PS group", 'Unknown')
                        END AS actual_group,
                        CASE
                            WHEN z.employee_id = '' OR z.employee_id IS NULL THEN NULL
                            ELSE COALESCE(pa1."PA", 'Unknown')
                        END AS actual_personnel_area,

                        GREATEST(z.valid_from, COALESCE(pa1.valid_from, z.valid_from), COALESCE(pa8.valid_from, z.valid_from)) AS seg_start,
                        LEAST(z.valid_to,      COALESCE(pa1.valid_to,   z.valid_to),   COALESCE(pa8.valid_to,   z.valid_to))   AS seg_end,

                        z.d_std_cost, z.d_other, z.d_fund, z.d_charge, z.d_tot_post,
                        z.d_cother, z.d_ccharges, z.d_local_host, z.d_local1, z.d_local2,
                        z.d_tot_non_post, z.d_total,

                        z.* EXCLUDE (
                            employee_id, position_id, zpost_group,
                            valid_from, valid_to,
                            d_std_cost, d_other, d_fund, d_charge, d_tot_post,
                            d_cother, d_ccharges, d_local_host, d_local1, d_local2,
                            d_tot_non_post, d_total
                        )

                    FROM zpost_clean z

                    LEFT JOIN '{file_pa1}' pa1
                        ON z.employee_id != ''
                        AND z.employee_id IS NOT NULL
                        AND z.employee_id = TRIM(pa1."Pers.No.")
                        AND z.valid_from <= pa1.valid_to
                        AND z.valid_to   >= pa1.valid_from

                    LEFT JOIN '{file_pa8}' pa8
                        ON z.employee_id != ''
                        AND z.employee_id IS NOT NULL
                        AND z.employee_id = TRIM(pa8."Pers.No.")
                        AND z.valid_from <= pa8.valid_to
                        AND z.valid_to   >= pa8.valid_from
                ),

                valid_segments AS (
                    SELECT
                        *,
                        date_diff('day', seg_start, seg_end) + 1 AS seg_days
                    FROM interval_joined
                    WHERE seg_start <= seg_end
                )

                SELECT
                    employee_id             AS "Personnel Number",
                    position_id             AS "Position",
                    zpost_group          AS "Planned PS Group",
                    actual_group         AS "Actual PS Group",
                    actual_personnel_area   AS "Actual Personnel Area",

                    seg_start               AS "Start_Date",
                    seg_end                 AS "End_Date",
                    seg_days                AS "Days in Segment",

                    ROUND(d_std_cost     * seg_days, 2) AS "Standard Cost",
                    ROUND(d_        * seg_days, 2) AS "Transfer to Reserves",
                    ROUND(d_fund     * seg_days, 2) AS "Transfer to Fund",
                    ROUND(d_charge          * seg_days, 2) AS "Transfer Reserve",
                    ROUND(d_tot_post     * seg_days, 2) AS "Total Post Cost",
                    ROUND(d_cother      * seg_days, 2) AS "Other Costs",
                    ROUND(d_ccharges         * seg_days, 2) AS "Other Charges",
                    ROUND(d_local_host   * seg_days, 2) AS "Local Cost",
                    ROUND(d_local1       * seg_days, 2) AS "Local Cost1",
                    ROUND(d_local2       * seg_days, 2) AS "Local Cost2",
                    ROUND(d_tot_non_post * seg_days, 2) AS "Total Non Post Cost",
                    ROUND(d_total        * seg_days, 2) AS "Total",

                    * EXCLUDE (
                        employee_id, position_id, zpost_group, actual_group, actual_personnel_area,
                        seg_start, seg_end, seg_days,
                        d_std_cost, d_other, d_fund, d_charge, d_tot_post, d_cother,
                        d_ccharges, d_local_host, d_local1, d_local2, d_tot_non_post, d_total
                    )

                FROM valid_segments
                ORDER BY position_id, employee_id, "Start_Date"

            ) TO '{output_path}' (FORMAT PARQUET, CODEC 'ZSTD');
        """

        query_conn.execute(temporal_query)
        final_rows = query_conn.execute(f"SELECT COUNT(*) FROM '{output_path}'").fetchone()[0]

        log_transform(
            meta_conn,
            table_name       = 'gold_hr_financial_summary',
            layer            = 'gold',
            source_file      = f"{file_zpost.name} + {file_pa1.name} + {file_pa8.name}",
            output_file      = output_path.name,
            input_row_count  = input_rows,
            output_row_count = final_rows
        )

        print(f"Success! Temporal Join Complete.")
        print(f"Final Gold File: {output_path.name}")
        print(f"Total Rows Generated: {final_rows}")

    except Exception as e:
        status = "failed"
        print(f"Fatal error during temporal join: {e}")
        raise

    finally:
        log_run(meta_conn, "build_hr_master", str(GOLD_DIR), final_rows, 1, status)
        query_conn.close()
        meta_conn.close()


if __name__ == "__main__":
    process_temporal_join()