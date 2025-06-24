SELECT DISTINCT
    STORE_ID,
    ASCENDER_ID,
    PRICE,
    QUANTITY,
    TIMESTAMP,
    ID
FROM {{ ref('read_sales_stores', flow='extract-load') }}