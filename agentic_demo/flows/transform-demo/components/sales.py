WITH joined_sales AS (
    SELECT
        ss.STORE_ID,
        ss.ASCENDER_ID,
        ss.PRICE,
        ss.QUANTITY,
        ss.TIMESTAMP,
        ss.ID,
        sv.VENDOR_ID
    FROM {{ ref('sales_stores') }} ss
    LEFT JOIN {{ ref('sales_website') }} sw
        ON ss.ASCENDER_ID = sw.ASCENDER_ID
    LEFT JOIN {{ ref('sales_vendors') }} sv
        ON ss.ID = sv.ID
),
daily_sales AS (
    SELECT
        STORE_ID,
        CAST(DATE_TRUNC('day', TIMESTAMP) AS DATE) AS SALE_DATE,
        SUM(PRICE * QUANTITY) AS total_sales
    FROM joined_sales
    GROUP BY STORE_ID, CAST(DATE_TRUNC('day', TIMESTAMP) AS DATE)
)
SELECT
    STORE_ID,
    SALE_DATE,
    total_sales,
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
ORDER BY STORE_ID, SALE_DATE