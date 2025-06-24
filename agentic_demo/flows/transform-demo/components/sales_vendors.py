SELECT DISTINCT
    ID,
    VENDOR_ID,
    TIMESTAMP
FROM {{ ref('read_sales_vendors', flow='extract-load') }}