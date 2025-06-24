{{
    config(
        materialized="table"
    )
}}

-- Clean and deduplicate sales_vendors data
SELECT
    QUANTITY,
    TIMESTAMP,
    ID,
    PRICE,
    TAX,
    VENDOR_ID,
    ROUTE_ID
FROM
    {{ ref('read_sales_vendors', flow='extract-load') }}
GROUP BY
    QUANTITY, TIMESTAMP, ID, PRICE, TAX, VENDOR_ID, ROUTE_ID

{{ with_test("not_null", column="TIMESTAMP") }}