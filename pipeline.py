from pyspark.sql.functions import col, when

from src.ingestion import *
from src.cleaning import *
from src.enrichment import *
from src.aggregations import *
from src.returns_analysis import *
from src.output import *


def main():

    spark = create_spark_session()

    # Ingestion
    orders_df = load_csv(spark, "data/orders.csv", orders_schema)
    customers_df = load_csv(spark, "data/customers.csv", customers_schema)
    order_items_df = load_csv(spark, "data/order_items.csv", order_items_schema)
    returns_df = load_csv(spark, "data/returns.csv", returns_schema)

    # Casting
    orders_df = cast_orders(orders_df)
    order_items_df = cast_order_items(order_items_df)
    returns_df = cast_returns(returns_df)

    # Rejected Records
    rejected_orders = get_rejected_orders(orders_df)
    rejected_items = get_rejected_order_items(order_items_df)
    rejected_returns = get_rejected_returns(returns_df)

    write_csv(rejected_orders, "rejected/orders")
    write_csv(rejected_items, "rejected/order_items")
    write_csv(rejected_returns, "rejected/returns")

    # Cleaning
    orders_df = clean_orders(orders_df)
    customers_df = clean_customers(customers_df)
    order_items_df = clean_order_items(order_items_df)
    returns_df = clean_returns(returns_df)

    # Orphans
    orphaned_items = isolate_orphaned_items(order_items_df, orders_df)
    write_csv(orphaned_items, "output/orphaned_items")

    # Enrichment
    # LEFT JOIN used to preserve all orders 
    enriched_orders = enrich_orders(
        orders_df,
        customers_df,
        order_items_df
    )

    # Derived Features
    enriched_orders = enriched_orders.withColumn(
        "net_amount",
        col("total_amount") * (1 - col("discount_pct") / 100)
    )

    enriched_orders = enriched_orders.withColumn(
        "negative_amount",
        when(col("total_amount") < 0, True).otherwise(False)
    )

    # Returns Analysis
    (
        returns_joined,
        category_return_rate,
        tier_return_rate,
        top_refund_customers
    ) = analyze_returns(returns_df, enriched_orders)

    returns_joined = returns_joined.withColumn(
        "refund_exceeds_order",
        col("refund_amount") > col("net_amount")
    )

    top_refund_customers = top_refund_customers.limit(10)

    # Data Quality
    dq_summary = enriched_orders.groupBy("negative_amount").count()
    dq_summary.show()

    write_csv(dq_summary, "output/dq_summary")

    # Aggregations
    ranked_customers = customer_lifetime_rank(enriched_orders)
    rolling_orders = rolling_7day_orders(enriched_orders)
    revenue_share = category_monthly_revenue_share(enriched_orders)

    # Output
    write_parquet(
        enriched_orders,
        "output/final_parquet",
        partitionBy=["order_year", "order_month"]
    )

    write_csv(ranked_customers, "output/ranked_customers")
    write_csv(rolling_orders, "output/rolling_orders")
    write_csv(revenue_share, "output/revenue_share")

    write_csv(category_return_rate, "output/category_return_rate")
    write_csv(tier_return_rate, "output/tier_return_rate")
    write_csv(top_refund_customers, "output/top_refund_customers")


    enriched_orders.explain(mode="formatted")

    spark.stop()

if __name__ == "__main__":
    main()