from pyspark.sql.functions import (
    col,
    lower,
    coalesce,
    to_date,
    when,
    lit
)


def normalize_dates(df, column_name):
    
    # returns null and coalesce falls through to the next candidate.
    return df.withColumn(
        column_name,
        coalesce(
            to_date(col(column_name), "MM/dd/yyyy"),  # primary format
            to_date(col(column_name), "yyyy-MM-dd"),  # ISO fallback
            to_date(col(column_name), "dd/MM/yyyy"),  # European fallback
        )
    )


def clean_orders(df):
    df = df.dropDuplicates()

    df = normalize_dates(df, "order_date")

    df = df.dropna(subset=["order_id", "customer_id"])

    df = df.withColumn(
        "discount_pct",
        coalesce(col("discount_pct"), lit(0.0))
    )

    df = df.withColumn(
        "is_negative_amount",
        when(col("total_amount") < 0, True).otherwise(False)
    )

    return df


def clean_customers(df):
    df = df.dropDuplicates()

    df = normalize_dates(df, "signup_date")

    df = df.withColumn(
        "customer_tier",
        lower(col("customer_tier"))
    )

    return df


def clean_order_items(df):
    return df.dropDuplicates()


def clean_returns(df):
    df = df.dropDuplicates()
    df = normalize_dates(df, "return_date")
    return df