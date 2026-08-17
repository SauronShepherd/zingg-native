"""Run verified Exact/Jaccard operations through a Spark Connect plugin."""

from pyspark.sql.connect.session import SparkSession

from zingg_native import Zingg


def main() -> None:
    spark = (
        SparkSession.builder.remote("sc://localhost:15002")
        .config("zingg.native.connect.plugin.loaded", "true")
        .getOrCreate()
    )
    try:
        zingg = Zingg(spark=spark, backend="connect")
        exact = zingg.exact(
            spark.sql("SELECT 'x' AS left_value, 'x' AS right_value UNION ALL SELECT 'x', 'y'"),
            "left_value", "right_value"
        ).collect()
        assert [row[2] for row in exact] == [1.0, 0.0], exact
        jaccard = zingg.jaccard(
            spark.sql("SELECT 'New York' AS left_value, 'new-york' AS right_value"),
            "left_value", "right_value"
        ).collect()
        assert jaccard[0][2] == 1.0, jaccard
        print({"exact": [tuple(row) for row in exact], "jaccard": [tuple(row) for row in jaccard]})
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
