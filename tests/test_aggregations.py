from pyspark.sql import SparkSession
from src.aggregations import customer_lifetime_rank


def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


def test_customer_lifetime_rank():
    s = spark()

    df = s.createDataFrame([
        ("c1", "Kenya", 100.0),
        ("c2", "Kenya", 200.0),
        ("c3", "Kenya", 50.0)
    ], ["customer_id", "country", "net_amount"])

    result = customer_lifetime_rank(df)

    assert "rank" in result.columns
    assert result.count() == 3