SELECT
    CASE
        WHEN TRIM("Start Date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("Start Date"), '%d.%m.%Y')::DATE
    END AS valid_from,
    CASE
        WHEN TRIM("End Date") IN ('', '00.00.0000') THEN NULL
        WHEN TRIM("End Date") = '31.12.9999' THEN '2099-12-31'::DATE
        ELSE STRPTIME(TRIM("End Date"), '%d.%m.%Y')::DATE
    END AS valid_to,
    TRIM("Cost Ctr")             AS cost_center,
    NULLIF(TRIM("Org.unit"), '') AS org_unit,
    * EXCLUDE ("Start Date", "End Date", "Cost Ctr", "Org.unit")
FROM '{source_file}'
{filters}