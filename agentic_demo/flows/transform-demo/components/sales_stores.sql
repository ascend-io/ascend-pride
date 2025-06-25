SELECT DISTINCT *
FROM {{ ref('read_sales_stores', flow='extract-load') }}

{{ with_test("not_null", column="timestamp") }}