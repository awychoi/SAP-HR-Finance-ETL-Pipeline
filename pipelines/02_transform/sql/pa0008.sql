SELECT
    CASE
        WHEN TRIM("Start Date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("Start Date"), '%d.%m.%Y')::DATE
    END AS valid_from,
    CASE
        WHEN TRIM("End Date") IN ('', '00.00.0000') THEN NULL
        ELSE STRPTIME(TRIM("End Date"), '%d.%m.%Y')::DATE
    END AS valid_to,
    * EXCLUDE ("Start Date", "End Date")
FROM '{source_file}'
{filters}