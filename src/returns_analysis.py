from pyspark.sql.functions import (
    col,
    count as spark_count,
    sum as spark_sum
)


def analyze_returns(returns_df, orders_df):

    # 1. Join returns with orders
    # LEFT JOIN used to preserve all orders
    joined = orders_df.join(
        returns_df,
        on="order_id",
        how="left"
    )

    # Return condition
    joined = joined.withColumn(
        "refund_exceeds_order",
        col("refund_amount") > col("net_amount")
    )

    # Return flag
    order_level = joined.withColumn(
        "is_returned",
        col("return_id").isNotNull()
    )

    # Overall return rate 
    overall_return_rate = order_level.agg(
        (spark_sum(col("is_returned").cast("int")) /
         spark_count("order_id")).alias("return_rate")
    )

    # Return rate by customer
    customer_return_rate = (
        order_level.groupBy("customer_id")
        .agg(
            (spark_sum(col("is_returned").cast("int")) /
             spark_count("order_id")).alias("return_rate")
        )
    )

    # Top refund customers
    top_refund_customers = (
        joined.groupBy("customer_id")
        .agg(
            spark_sum("refund_amount").alias("total_refund")
        )
        .orderBy(col("total_refund").desc())
        .limit(10)
    )

    return (
        joined,
        overall_return_rate,
        customer_return_rate,
        top_refund_customers
    )