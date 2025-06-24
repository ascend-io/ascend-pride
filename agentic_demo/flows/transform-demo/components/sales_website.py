SELECT DISTINCT
    ASCENDER_ID,
    ID,
    TIMESTAMP
FROM {{ ref('read_sales_website', flow='extract-load') }}