-- Cleans and deduplicates sales_stores data from extract-load flow
SELECT DISTINCT
    ASCENDER_ID,
    STORE_ID,
    QUANTITY,
    ROUTE_ID,
    TAX,
    PRICE,
    TIMESTAMP,
    ID
FROM {{ ref('read_sales_stores', flow='extract-load') }}

{{ with_test('not_null', column='TIMESTAMP') }}