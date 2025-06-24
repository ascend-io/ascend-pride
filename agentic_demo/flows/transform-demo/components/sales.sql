WITH
-- Select only needed columns from sales_stores
sales_stores_sel AS (
    SELECT
        STORE_ID, ASCENDER_ID, PRICE, QUANTITY, TIMESTAMP, ID
    FROM {{ ref('sales_stores') }}
),
-- Select only needed columns from sales_website
sales_website_sel AS (
    SELECT
        ASCENDER_ID, ID
    FROM {{ ref('sales_website') }}
),
-- Select only needed columns from sales_vendors
sales_vendors_sel AS (
    SELECT
        ID, VENDOR_ID
    FROM {{ ref('sales_vendors') }}
),
-- Join sales_stores and sales_website on ASCENDER_ID
stores_web AS (
    SELECT
        s.STORE_ID, s.ASCENDER_ID, s.PRICE, s.QUANTITY, s.TIMESTAMP, s.ID
    FROM sales_stores_sel s
    LEFT JOIN sales_website_sel w
        ON s.ASCENDER_ID = w.ASCENDER_ID
),
-- Join the result to sales_vendors on ID
joined AS (
    SELECT
        sw.STORE_ID, sw.ASCENDER_ID, sw.PRICE, sw.QUANTITY, sw.TIMESTAMP, sw.ID, v.VENDOR_ID
    FROM stores_web sw
    LEFT JOIN sales_vendors_sel v
        ON sw.ID = v.ID
),
-- Calculate total sales per store per day
daily_sales AS (
    SELECT
        STORE_ID,
        CAST(TIMESTAMP AS DATE) AS SALE_DATE,
        SUM(PRICE * QUANTITY) AS total_sales
    FROM joined
    GROUP BY STORE_ID, CAST(TIMESTAMP AS DATE)
)
-- Final select: add 7-day rolling sum and sales rank
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
ORDER BY STORE_ID, SALE_DATE;