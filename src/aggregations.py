from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    rank as spark_rank,
    count as spark_count,
    lit
)

# Customer Lifetime Rank
def customer_lifetime_rank(df):

    # Aggregate lifetime spend per customer
    spend_df = df.groupBy(
        "customer_id",
        "country"
    ).agg(
        spark_sum("net_amount").alias("lifetime_spend")
    )

    # Window for ranking within each country
    window_spec = Window.partitionBy(
        "country"
    ).orderBy(
        col("lifetime_spend").desc()
    )

    return spend_df.withColumn(
        "rank",
        spark_rank().over(window_spec)
    )

# 7-Day Rolling Orders
def rolling_7day_orders(df):

    # timestamp type for window operations
    df = df.withColumn(
        "order_ts",
        col("order_date").cast("timestamp")
    )

    # Last 7 days
    window_spec = Window.partitionBy(
        "customer_id"
    ).orderBy(
        col("order_ts").cast("long")
    ).rangeBetween(-7 * 86400, 0)

    return df.withColumn(
        "rolling_7day_orders",
        spark_count(lit(1)).over(window_spec)
    )

# Monthly Revenue Share
def category_monthly_revenue_share(df):

    # Window for monthly totals
    monthly_window = Window.partitionBy(
        "order_year",
        "order_month"
    )

    # Window for category revenue per month
    category_window = Window.partitionBy(
        "order_year",
        "order_month",
        "category"
    )

    df = df.withColumn(
        "category_revenue",
        spark_sum("net_amount").over(category_window)
    )

    df = df.withColumn(
        "monthly_total",
        spark_sum("net_amount").over(monthly_window)
    )

    df = df.withColumn(
        "revenue_share_pct",
        (col("category_revenue") / col("monthly_total")) * 100
    )

    return df.select(
        "order_year",
        "order_month",
        "category",
        "category_revenue",
        "monthly_total",
        "revenue_share_pct"
    ).dropDuplicates()