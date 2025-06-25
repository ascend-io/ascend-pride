WITH stores_web AS (
    SELECT
        s.STORE_ID,
        s.ASCENDER_ID,
        s.PRICE,
        s.QUANTITY,
        s.TIMESTAMP,
        s.ID
    FROM {{ ref('sales_stores') }} s
    LEFT JOIN {{ ref('sales_website') }} w
        ON s.ASCENDER_ID = w.ASCENDER_ID
),
joined AS (
    SELECT
        sw.STORE_ID,
        sw.ASCENDER_ID,
        sw.PRICE,
        sw.QUANTITY,
        sw.TIMESTAMP,
        sw.ID,
        v.VENDOR_ID
    FROM stores_web sw
    LEFT JOIN {{ ref('sales_vendors') }} v
        ON sw.ID = v.ID
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
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS sales_rank
FROM daily_sales
ORDER BY STORE_ID, SALE_DATE