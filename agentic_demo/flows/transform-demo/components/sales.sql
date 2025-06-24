-- Joins cleaned sales tables, computes daily sales, rolling 7d sum, and sales rank
WITH joined AS (
    SELECT
        s.STORE_ID,
        s.ASCENDER_ID,
        s.PRICE,
        s.QUANTITY,
        s.TIMESTAMP,
        s.ID,
        w.ID AS WEBSITE_ID,
        v.VENDOR_ID
    FROM {{ ref('sales_stores') }} s
    LEFT JOIN {{ ref('sales_website') }} w
        ON s.ASCENDER_ID = w.ASCENDER_ID
    LEFT JOIN {{ ref('sales_vendors') }} v
        ON s.ID = v.ID
),
daily_sales AS (
    SELECT
        STORE_ID,
        CAST(TIMESTAMP AS DATE) AS SALE_DATE,
        SUM(PRICE * QUANTITY) AS total_sales
    FROM joined
    GROUP BY STORE_ID, CAST(TIMESTAMP AS DATE)
)
SELECT
    STORE_ID,
    total_sales,
    sales_rank,
    SALE_DATE,
    rolling_7d_sales
FROM (
    SELECT
        STORE_ID,
        total_sales,
        SALE_DATE,
        SUM(total_sales) OVER (
            PARTITION BY STORE_ID
            ORDER BY SALE_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_sales,
        RANK() OVER (
            PARTITION BY STORE_ID
            ORDER BY total_sales DESC
        ) AS sales_rank
    FROM daily_sales
)