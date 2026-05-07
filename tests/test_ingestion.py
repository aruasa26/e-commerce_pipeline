from pyspark.sql import SparkSession
from src.ingestion import load_csv, orders_schema, get_rejected_orders


def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


def test_load_csv_and_schema():
    s = spark()

    df = s.createDataFrame([
        ("1", "100", "2024-01-01", "shipped", "200", "10"),
        ("2", None, "2024-01-01", "shipped", "300", "5")
    ], ["order_id","customer_id","order_date","status","total_amount","discount_pct"])

    rejected = get_rejected_orders(df)

    assert rejected.filter(rejected.customer_id.isNull()).count() > 0