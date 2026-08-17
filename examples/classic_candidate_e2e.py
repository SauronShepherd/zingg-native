"""Run the shared-core candidate phase through the real Classic/Py4J boundary."""

import os

from pyspark.sql import SparkSession

from zingg_native import Zingg


def main() -> None:
    jar = os.environ.get("ZINGG_NATIVE_CORE_JAR")
    if not jar:
        raise SystemExit("Set ZINGG_NATIVE_CORE_JAR to the built core JAR")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("zingg-native-classic-candidate-e2e")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraClassPath", jar)
        .config("spark.executor.extraClassPath", jar)
        .getOrCreate()
    )
    try:
        source = spark.sql(
            """
            SELECT 'a' AS record_id, 'Alice' AS name, 'Madrid' AS city
            UNION ALL SELECT 'b', 'alice', 'madrid'
            UNION ALL SELECT 'c', 'Bob', 'Madrid'
            """
        )
        zingg = Zingg(spark=spark)
        assert zingg.status()["backend"] == "classic-py4j"
        pairs = zingg.find_training_data(source, ["name", "city"], "record_id")
        rows = [tuple(row) for row in pairs.collect()]
        assert len(rows) == 1, rows
        assert rows[0][1:] == ("a", "c", 0.0, 1.0, 0.5, None), rows
        labeled = zingg.label(pairs, match_threshold=0.5)
        labeled_rows = [tuple(row) for row in labeled.collect()]
        assert labeled_rows[0][-1] == 1, labeled_rows
        print({"status": zingg.status(), "rows": rows, "labeled": labeled_rows})
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
