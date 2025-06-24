{{
    config(
        materialized="table"
    )
}}

-- Clean and deduplicate sales_stores data
SELECT
    ASCENDER_ID,
    STORE_ID,
    QUANTITY,
    ROUTE_ID,
    TAX,
    PRICE,
    TIMESTAMP,
    ID
FROM
    {{ ref('read_sales_stores', flow='extract-load') }}
GROUP BY
    ASCENDER_ID, STORE_ID, QUANTITY, ROUTE_ID, TAX, PRICE, TIMESTAMP, ID

{{ with_test("not_null", column="TIMESTAMP") }}