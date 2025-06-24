import ibis

from ascend.resources import ref, transform, test
from ascend.application.context import ComponentExecutionContext

@transform(
    inputs=[ref("read_sales_stores", flow="extract-load")],
    materialized="table",
    tests=[test("not_null", column="timestamp")],
)
def sales_stores(
    read_sales_stores: ibis.Table, context: ComponentExecutionContext
) -> ibis.Table:
    # Inline the cleaning logic
    if ibis.get_backend(read_sales_stores).name == "snowflake":
        cleaned = read_sales_stores.rename("ALL_CAPS").distinct()
    else:
        cleaned = read_sales_stores.rename("snake_case").distinct()
    return cleaned

