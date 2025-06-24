import ibis

from ascend.resources import ref, transform, test
from ascend.application.context import ComponentExecutionContext


@transform(
    inputs=[
        ref(
            "read_sales_website",
            flow="extract-load",
            reshape={"time": {"column": "timestamp", "granularity": "day"}},
        )
    ],
    materialized="table",
    tests=[test("not_null", column="timestamp")],
)
def sales_website(
    read_sales_website: ibis.Table, context: ComponentExecutionContext
) -> ibis.Table:
    # Inline the cleaning logic
    if ibis.get_backend(read_sales_website).name == "snowflake":
        cleaned = read_sales_website.rename("ALL_CAPS").distinct()
    else:
        cleaned = read_sales_website.rename("snake_case").distinct()
    return cleaned