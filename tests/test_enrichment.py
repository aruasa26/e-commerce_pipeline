from pyspark.sql import SparkSession
from src.enrichment import enrich_orders


def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


def test_enrichment_net_amount():
    s = spark()

    orders = s.createDataFrame([
        ("1", "c1", "2024-01-01", 100.0, 10.0)
    ], ["order_id", "customer_id", "order_date", "total_amount", "discount_pct"])

    customers = s.createDataFrame([
        ("c1", "Kenya")
    ], ["customer_id", "country"])

    items = s.createDataFrame([
        ("1", "p1", 2, 50.0, "tech")
    ], ["order_id", "product_id", "quantity", "unit_price", "category"])

    result = enrich_orders(orders, customers, items)

    assert "net_amount" in result.columns
    assert "order_year" in result.columns
    assert "order_month" in result.columns

    row = result.collect()[0]

    # 100 * (1 - 10/100) = 90
    assert row["net_amount"] == 90.0

    # sanity checks
    assert result.count() > 0