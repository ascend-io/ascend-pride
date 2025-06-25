{{
    config(
        materialized="table"
    )
}}

SELECT DISTINCT
    "ID",
    "VENDOR_ID",
    "TIMESTAMP"
FROM {{ ref('read_sales_vendors', flow='extract-load') }}

{{ with_test("not_null", column="TIMESTAMP") }}