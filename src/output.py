def write_parquet(df, path, partitionBy=None):

    writer = df.write.mode("overwrite")

    if partitionBy:
        writer = writer.partitionBy(*partitionBy)

    writer.parquet(path)


def write_csv(df, path):

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )