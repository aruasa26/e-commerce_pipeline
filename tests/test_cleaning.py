from pyspark.sql import SparkSession
from src.ingestion import cast_orders


def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


def test_cast_orders():
    s = spark()

    df = s.createDataFrame([
        ("1", "100", "2024-01-01", "shipped", "200", "10")
    ], ["order_id","customer_id","order_date","status","total_amount","discount_pct"])

    result = cast_orders(df)

    assert result.schema["total_amount"].dataType.typeName() == "double"
    assert result.schema["discount_pct"].dataType.typeName() == "double"