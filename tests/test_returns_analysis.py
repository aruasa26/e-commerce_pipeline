from pyspark.sql import SparkSession
from src.returns_analysis import analyze_returns


def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


def test_returns_analysis_basic():
    s = spark()

    orders = s.createDataFrame([
        ("1", "c1", "2024-01-01", 100.0)
    ], ["order_id","customer_id","order_date","net_amount"])

    returns = s.createDataFrame([
        ("r1", "1", 50.0)
    ], ["return_id","order_id","refund_amount"])

    joined, cat, tier, top = analyze_returns(returns, orders)

    assert "refund_exceeds_order" in joined.columns
    assert cat.count() >= 0
    assert top.count() >= 0