from pyspark.sql.functions import (
    col,
    year,
    month,
    broadcast,
    coalesce,
    lit
)


def isolate_orphaned_items(order_items_df, orders_df):
    return order_items_df.join(
        orders_df.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )


def enrich_orders(orders_df, customers_df, order_items_df):

    valid_order_items = order_items_df.join(
        orders_df.select("order_id").distinct(),
        on="order_id",
        how="inner"
    ).dropDuplicates()

    enriched = (
        orders_df.alias("o")
        .join(
            broadcast(customers_df.alias("c")),
            on="customer_id",
            how="left"
        )
        .join(
            valid_order_items.alias("oi"),
            on="order_id",
            how="inner"
        )
    )

    enriched = enriched.withColumn(
    "net_amount",
    col("o.total_amount") * (
        1 - (coalesce(col("o.discount_pct"), lit(0.0)) / 100)
    )
)

    enriched = enriched.withColumn(
    "order_year",
    year(col("o.order_date"))
    ).withColumn(
    "order_month",
    month(col("o.order_date"))
)

    enriched = enriched.dropDuplicates()

    return enriched