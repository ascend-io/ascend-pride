import ibis
from ibis import window
from ascend.resources import ref, transform, test
from ascend.application.context import ComponentExecutionContext

@transform(
    inputs=[
        ref("sales_stores"),
        ref("sales_website"),
        ref("sales_vendors"),
    ]
)
def sales(
    sales_stores: ibis.Table,
    sales_website: ibis.Table,
    sales_vendors: ibis.Table,
    context: ComponentExecutionContext,
) -> ibis.Table:
    # Select only the columns you need from each table to avoid name collisions
    stores_cols = [
        "STORE_ID", "ASCENDER_ID", "PRICE", "QUANTITY", "TIMESTAMP", "ID"
    ]
    website_cols = [
        "ASCENDER_ID", "ID"
    ]
    vendors_cols = [
        "ID", "VENDOR_ID"
    ]

    sales_stores_sel = sales_stores[stores_cols]
    sales_website_sel = sales_website[website_cols]
    sales_vendors_sel = sales_vendors[vendors_cols]

    # Join sales_stores and sales_website on ASCENDER_ID
    stores_web = sales_stores_sel.join(
        sales_website_sel,
        sales_stores_sel.ASCENDER_ID == sales_website_sel.ASCENDER_ID,
        how="left"
    )[
        # Only keep columns from sales_stores (left) and any needed from sales_website (right)
        "STORE_ID", "ASCENDER_ID", "PRICE", "QUANTITY", "TIMESTAMP", "ID"
    ]

    # Join the result to sales_vendors on ID
    joined = stores_web.join(
        sales_vendors_sel,
        stores_web.ID == sales_vendors_sel.ID,
        how="left"
    )[
        # Only keep columns you want, avoiding ID_right
        "STORE_ID", "ASCENDER_ID", "PRICE", "QUANTITY", "TIMESTAMP", "ID", "VENDOR_ID"
    ]

    # Calculate total sales per store per day
    daily_sales = joined.group_by([
        joined.STORE_ID,
        joined.TIMESTAMP.date().name("SALE_DATE")
    ]).aggregate(
        total_sales=(joined.PRICE * joined.QUANTITY).sum()
    )

    # Define a 7-day rolling window for each store
    rolling_win = window(
        preceding=6,
        order_by=daily_sales.SALE_DATE,
        group_by=daily_sales.STORE_ID,
    )

    # Add 7-day rolling sum and rank
    result = daily_sales.mutate(
    rolling_7d_sales=daily_sales.total_sales.sum().over(rolling_win),
    sales_rank=daily_sales.total_sales.rank().over(rolling_win)
    )

    return result
