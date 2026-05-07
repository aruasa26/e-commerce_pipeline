from pyspark.sql import SparkSession
from src.output import write_parquet, write_csv


def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


def test_output_write_parquet(tmp_path):
    s = spark()

    df = s.createDataFrame([
        ("1", 100.0)
    ], ["order_id","net_amount"])

    path = tmp_path / "parquet_out"

    write_parquet(df, str(path))

    # If no exception- success
    assert True


def test_output_write_csv(tmp_path):
    s = spark()

    df = s.createDataFrame([
        ("1", 100.0)
    ], ["order_id","net_amount"])

    path = tmp_path / "csv_out"

    write_csv(df, str(path))

    assert True