{{
    config(
        materialized="table"
    )
}}

SELECT DISTINCT
    "ASCENDER_ID",
    "ID",
    "TIMESTAMP"
FROM {{ ref('read_sales_website', flow='extract-load', reshape={
    "time": {
        "column": "timestamp",
        "granularity": "day"
    }
}) }}

{{ with_test("not_null", column="TIMESTAMP") }}