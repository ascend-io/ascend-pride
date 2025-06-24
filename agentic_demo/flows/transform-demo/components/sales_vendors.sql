-- Cleans and deduplicates sales_vendors data from extract-load flow
SELECT DISTINCT
    QUANTITY,
    TIMESTAMP,
    ID,
    TAX,
    PRICE,
    VENDOR_ID,
    ROUTE_ID
FROM {{ ref('read_sales_vendors', flow='extract-load') }}

{{ with_test('not_null', column='TIMESTAMP') }}