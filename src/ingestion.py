from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("EcommerceETLPipeline")
        .master("local[*]")
        # Disable ANSI mode so to_date/to_timestamp return null
        # instead of throwing on unparseable strings. Required for
        # normalize_dates coalesce-fallback logic to work correctly.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("discount_pct", StringType(), True)
])


customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("email", StringType(), True)
])


order_items_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("category", StringType(), True)
])


returns_schema = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("refund_amount", StringType(), True)
])


def load_csv(spark, path, schema):
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )


def cast_orders(df):
    return (
        df.withColumn("total_amount", col("total_amount").cast("double"))
          .withColumn("discount_pct", col("discount_pct").cast("double"))
    )


def cast_order_items(df):
    return (
        df.withColumn("quantity", col("quantity").cast("int"))
          .withColumn("unit_price", col("unit_price").cast("double"))
    )


def cast_returns(df):
    return (
        df.withColumn("refund_amount", col("refund_amount").cast("double"))
    )


def get_rejected_orders(df):
    return df.filter(
        col("customer_id").isNull() |
        col("total_amount").isNull()
    )

def get_rejected_order_items(df):
    return df.filter(
        col("quantity").isNull() |
        col("unit_price").isNull()
    )


def get_rejected_returns(df):
    return df.filter(col("refund_amount").isNull())