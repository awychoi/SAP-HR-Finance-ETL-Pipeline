SELECT
    CASE
        WHEN TRIM("Start date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("Start date"), '%d.%m.%Y')::DATE
    END AS valid_from,
    CASE
        WHEN TRIM("End Date") IN ('', '00.00.0000') THEN NULL
        WHEN TRIM("End Date") = '31.12.9999' THEN '2099-12-31'::DATE
        ELSE STRPTIME(TRIM("End Date"), '%d.%m.%Y')::DATE
    END AS valid_to,

    CAST(REPLACE(NULLIF(TRIM("Standard Cost"),                  ''), ',', '') AS DECIMAL(18,2)) AS "Standard Cost",
    CAST(REPLACE(NULLIF(TRIM("Transfer to Reserves"),      ''), ',', '') AS DECIMAL(18,2)) AS "Transfer to Reserves",
    CAST(REPLACE(NULLIF(TRIM("Transfer to Fund"),    ''), ',', '') AS DECIMAL(18,2)) AS "Transfer to Fund",
    CAST(REPLACE(NULLIF(TRIM("Transfer Reserve"),        ''), ',', '') AS DECIMAL(18,2)) AS "Transfer Reserve",
    CAST(REPLACE(NULLIF(TRIM("Total Post Cost"),                ''), ',', '') AS DECIMAL(18,2)) AS "Total Post Cost",
    CAST(REPLACE(NULLIF(TRIM("Other Costs"),       ''), ',', '') AS DECIMAL(18,2)) AS "Other Costs",
    CAST(REPLACE(NULLIF(TRIM("Other Charges"),                ''), ',', '') AS DECIMAL(18,2)) AS "Other Charges",
    CAST(REPLACE(NULLIF(TRIM("Local Cost"),      ''), ',', '') AS DECIMAL(18,2)) AS "Local Cost",
    CAST(REPLACE(NULLIF(TRIM("Local Cost1"),            ''), ',', '') AS DECIMAL(18,2)) AS "Local Cost1",
    CAST(REPLACE(NULLIF(TRIM("Local Cost2 (Optional)"), ''), ',', '') AS DECIMAL(18,2)) AS "Local Cost2 (Optional)",
    CAST(REPLACE(NULLIF(TRIM("Total Non Post Cost"),            ''), ',', '') AS DECIMAL(18,2)) AS "Total Non Post Cost",
    CAST(REPLACE(NULLIF(TRIM("Total"),                          ''), ',', '') AS DECIMAL(18,2)) AS "Total",
    * EXCLUDE (
        "Start date", "End Date", "Adjusted Start Date", "Adjusted End Date",
        "Standard Cost", "Transfer to Reserves", "Transfer to Fund",
        "Transfer Reserve", "Total Post Cost", "Other Costs",
        "Other Charges", "Local Cost", "Local Cost1",
        "Local Cost2 (Optional)", "Total Non Post Cost", "Total"
    )
FROM '{source_file}'
{filters}