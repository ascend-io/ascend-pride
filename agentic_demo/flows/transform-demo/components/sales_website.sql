{{
    config(
        materialized="table"
    )
}}

-- Clean and deduplicate sales_website data, partitioned by day
SELECT
    ASCENDER_ID,
    PRICE,
    TAX,
    QUANTITY,
    TIMESTAMP,
    ID,
    ROUTE_ID
FROM
    {{
        ref(
            'read_sales_website',
            flow='extract-load',
            reshape={"time": {"column": "timestamp", "granularity": "day"}}
        )
    }}
GROUP BY
    ASCENDER_ID, PRICE, TAX, QUANTITY, TIMESTAMP, ID, ROUTE_ID

{{ with_test("not_null", column="TIMESTAMP") }}