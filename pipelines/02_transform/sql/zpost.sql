SELECT
    CASE
        WHEN TRIM("Start date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("Start date"), '%d.%m.%Y')::DATE
    END AS valid_from,
    CASE
        WHEN TRIM("End Date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("End Date"), '%d.%m.%Y')::DATE
    END AS valid_to,
    CASE
        WHEN TRIM("Adjusted Start Date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("Adjusted Start Date"), '%d.%m.%Y')::DATE
    END AS adj_valid_from,
    CASE
        WHEN TRIM("Adjusted End Date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("Adjusted End Date"), '%d.%m.%Y')::DATE
    END AS adj_valid_to,
    CAST(REPLACE(NULLIF(TRIM("Standard Cost"),                  ''), ',', '') AS DECIMAL(18,2)) AS "Standard Cost",
    CAST(REPLACE(NULLIF(TRIM("Transfer to Ashi Reserves"),      ''), ',', '') AS DECIMAL(18,2)) AS "Transfer to Ashi Reserves",
    CAST(REPLACE(NULLIF(TRIM("Transfer to Separation Fund"),    ''), ',', '') AS DECIMAL(18,2)) AS "Transfer to Separation Fund",
    CAST(REPLACE(NULLIF(TRIM("Transfer to MIP Reserve"),        ''), ',', '') AS DECIMAL(18,2)) AS "Transfer to MIP Reserve",
    CAST(REPLACE(NULLIF(TRIM("Total Post Cost"),                ''), ',', '') AS DECIMAL(18,2)) AS "Total Post Cost",
    CAST(REPLACE(NULLIF(TRIM("Central Cost Attribution"),       ''), ',', '') AS DECIMAL(18,2)) AS "Central Cost Attribution",
    CAST(REPLACE(NULLIF(TRIM("GSSC Chargeback"),                ''), ',', '') AS DECIMAL(18,2)) AS "GSSC Chargeback",
    CAST(REPLACE(NULLIF(TRIM("Local Hosting Office Cost"),      ''), ',', '') AS DECIMAL(18,2)) AS "Local Hosting Office Cost",
    CAST(REPLACE(NULLIF(TRIM("Local Service Cost1"),            ''), ',', '') AS DECIMAL(18,2)) AS "Local Service Cost1",
    CAST(REPLACE(NULLIF(TRIM("Local Service Cost2 (Optional)"), ''), ',', '') AS DECIMAL(18,2)) AS "Local Service Cost2 (Optional)",
    CAST(REPLACE(NULLIF(TRIM("Total Non Post Cost"),            ''), ',', '') AS DECIMAL(18,2)) AS "Total Non Post Cost",
    CAST(REPLACE(NULLIF(TRIM("Total"),                          ''), ',', '') AS DECIMAL(18,2)) AS "Total",
    * EXCLUDE (
        "Start date", "End Date", "Adjusted Start Date", "Adjusted End Date",
        "Standard Cost", "Transfer to Ashi Reserves", "Transfer to Separation Fund",
        "Transfer to MIP Reserve", "Total Post Cost", "Central Cost Attribution",
        "GSSC Chargeback", "Local Hosting Office Cost", "Local Service Cost1",
        "Local Service Cost2 (Optional)", "Total Non Post Cost", "Total"
    )
FROM '{source_file}'
{filters}