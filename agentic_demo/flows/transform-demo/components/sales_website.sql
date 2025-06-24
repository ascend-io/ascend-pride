-- Cleans and deduplicates sales_website data from extract-load flow, partitioned by day
SELECT DISTINCT
    ASCENDER_ID,
    PRICE,
    TAX,
    QUANTITY,
    TIMESTAMP,
    ID,
    ROUTE_ID
FROM {{ ref('read_sales_website', flow='extract-load', reshape={"time": {"column": "TIMESTAMP", "granularity": "day"}}) }}

{{ with_test('not_null', column='TIMESTAMP') }}