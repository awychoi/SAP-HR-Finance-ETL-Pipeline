-- pipelines/03_aggregate/sql/temporal_join.sql
-- Temporal join with overlapping date range logic
-- Dependencies: {zpost}, {pa1}, {pa8}

WITH

zpost_clean AS (
    SELECT
        TRIM("Personnel Number")     AS employee_id,
        "Position"                   AS position_id,
        "Position Pay Scale Group"   AS zpost_ps_group,
        adj_valid_from,
        adj_valid_to,

        CAST("Standard Cost"                  AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_std_cost,
        CAST("Transfer to Ashi Reserves"      AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_ashi,
        CAST("Transfer to Separation Fund"    AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_sep_fund,
        CAST("Transfer to MIP Reserve"        AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_mip,
        CAST("Total Post Cost"                AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_tot_post,
        CAST("Central Cost Attribution"       AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_central,
        CAST("GSSC Chargeback"                AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_gssc,
        CAST("Local Hosting Office Cost"      AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_local_host,
        CAST("Local Service Cost1"            AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_local1,
        CAST("Local Service Cost2 (Optional)" AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_local2,
        CAST("Total Non Post Cost"            AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_tot_non_post,
        CAST("Total"                          AS DECIMAL(18,2)) / NULLIF(CAST("Number of Days" AS INTEGER), 0) AS d_total,

        * EXCLUDE (
            "Personnel Number", "Position", "Position Pay Scale Group",
            "Standard Cost", "Transfer to Ashi Reserves", "Transfer to Separation Fund",
            "Transfer to MIP Reserve", "Total Post Cost", "Central Cost Attribution",
            "GSSC Chargeback", "Local Hosting Office Cost", "Local Service Cost1",
            "Local Service Cost2 (Optional)", "Total Non Post Cost", "Total",
            "adj_valid_from", "adj_valid_to"
        )
    FROM '{zpost}'
    WHERE adj_valid_from IS NOT NULL AND adj_valid_to IS NOT NULL
),

interval_joined AS (
    SELECT
        z.employee_id,
        z.position_id,
        z.zpost_ps_group,

        CASE
            WHEN z.employee_id = '' OR z.employee_id IS NULL THEN NULL
            ELSE COALESCE(pa8."PS group", 'Unknown')
        END AS actual_ps_group,
        CASE
            WHEN z.employee_id = '' OR z.employee_id IS NULL THEN NULL
            ELSE COALESCE(pa1."PA", 'Unknown')
        END AS actual_personnel_area,
        CASE
            WHEN z.employee_id = '' OR z.employee_id IS NULL THEN NULL
            ELSE pa1.valid_from
        END AS pa1_valid_from,
        CASE
            WHEN z.employee_id = '' OR z.employee_id IS NULL THEN NULL
            ELSE pa1.valid_to
        END AS pa1_valid_to,

        GREATEST(z.adj_valid_from, COALESCE(pa1.valid_from, z.adj_valid_from), COALESCE(pa8.valid_from, z.adj_valid_from)) AS seg_start,
        LEAST(z.adj_valid_to,      COALESCE(pa1.valid_to,   z.adj_valid_to),   COALESCE(pa8.valid_to,   z.adj_valid_to))   AS seg_end,

        z.d_std_cost, z.d_ashi, z.d_sep_fund, z.d_mip, z.d_tot_post,
        z.d_central, z.d_gssc, z.d_local_host, z.d_local1, z.d_local2,
        z.d_tot_non_post, z.d_total,

        z.* EXCLUDE (
            employee_id, position_id, zpost_ps_group,
            adj_valid_from, adj_valid_to,
            d_std_cost, d_ashi, d_sep_fund, d_mip, d_tot_post,
            d_central, d_gssc, d_local_host, d_local1, d_local2,
            d_tot_non_post, d_total
        )

    FROM zpost_clean z

    LEFT JOIN '{pa1}' pa1
        ON z.employee_id != ''
        AND z.employee_id IS NOT NULL
        AND z.employee_id = TRIM(pa1."Pers.No.")
        AND z.adj_valid_from <= pa1.valid_to
        AND z.adj_valid_to   >= pa1.valid_from

    LEFT JOIN '{pa8}' pa8
        ON z.employee_id != ''
        AND z.employee_id IS NOT NULL
        AND z.employee_id = TRIM(pa8."Pers.No.")
        AND z.adj_valid_from <= pa8.valid_to
        AND z.adj_valid_to   >= pa8.valid_from
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
    zpost_ps_group          AS "Planned PS Group",
    actual_ps_group         AS "Actual PS Group",
    actual_personnel_area   AS "Actual Personnel Area",
    pa1_valid_from          AS "PA0001 Valid From",
    pa1_valid_to            AS "PA0001 Valid To",

    seg_start               AS "Adj_Start_Date",
    seg_end                 AS "Adj_End_Date",
    seg_days                AS "Days in Segment",

    ROUND(d_std_cost     * seg_days, 2) AS "Standard Cost",
    ROUND(d_ashi         * seg_days, 2) AS "Transfer to Ashi Reserves",
    ROUND(d_sep_fund     * seg_days, 2) AS "Transfer to Separation Fund",
    ROUND(d_mip          * seg_days, 2) AS "Transfer to MIP Reserve",
    ROUND(d_tot_post     * seg_days, 2) AS "Total Post Cost",
    ROUND(d_central      * seg_days, 2) AS "Central Cost Attribution",
    ROUND(d_gssc         * seg_days, 2) AS "GSSC Chargeback",
    ROUND(d_local_host   * seg_days, 2) AS "Local Hosting Office Cost",
    ROUND(d_local1       * seg_days, 2) AS "Local Service Cost1",
    ROUND(d_local2       * seg_days, 2) AS "Local Service Cost2",
    ROUND(d_tot_non_post * seg_days, 2) AS "Total Non Post Cost",
    ROUND(d_total        * seg_days, 2) AS "Total",

    * EXCLUDE (
        employee_id, position_id, zpost_ps_group, actual_ps_group, actual_personnel_area, pa1_valid_from, pa1_valid_to,
        seg_start, seg_end, seg_days,
        d_std_cost, d_ashi, d_sep_fund, d_mip, d_tot_post, d_central,
        d_gssc, d_local_host, d_local1, d_local2, d_tot_non_post, d_total
    )

FROM valid_segments
ORDER BY position_id, employee_id, "Adj_Start_Date"
