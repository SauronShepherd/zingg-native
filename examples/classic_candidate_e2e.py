"""Run the shared-core candidate phase through the real Classic/Py4J boundary."""

import json
import os
import socketserver

if not hasattr(socketserver, "UnixStreamServer"):
    # Spark 4.1's Windows client imports this Unix-only symbol during startup.
    socketserver.UnixStreamServer = socketserver.TCPServer  # type: ignore[attr-defined]

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
            UNION ALL SELECT 'c', 'Bob', 'Barcelona'
            """
        )
        zingg = Zingg(spark=spark)
        status = zingg.status()
        assert status["backend"] == "classic-py4j"
        assert status["capabilities"]["phases"] == ["preprocess", "findTrainingData", "buildTrainingPairs", "label", "updateLabel"]
        jaccard_rows = zingg.jaccard(
            spark.sql("SELECT 'New York' AS left_value, 'new-york' AS right_value"),
            "left_value", "right_value", "z_jaccard"
        ).select("z_jaccard").collect()
        jaro_rows = zingg.jaro(
            spark.sql("SELECT 'MARTHA' AS left_value, 'MARHTA' AS right_value"),
            "left_value", "right_value", "z_jaro"
        ).select("z_jaro").collect()
        assert abs(jaccard_rows[0][0] - 1.0) < 1e-12
        assert abs(jaro_rows[0][0] - 0.9444444444444445) < 1e-12
        normalized = zingg.preprocess(source, "CASE_NORMALIZE", ["name", "city"])
        pairs = zingg.find_training_data(normalized, ["name", "city"], "record_id")
        rows = [tuple(row) for row in pairs.collect()]
        assert len(rows) == 1, rows
        assert rows[0][1:] == ("a", "b", 1.0, 1.0, 1.0, None), rows
        labeled = zingg.label(pairs, match_threshold=0.5)
        labeled_rows = [tuple(row) for row in labeled.collect()]
        assert labeled_rows[0][-1] == 1, labeled_rows
        explicit = spark.sql("SELECT 'missing' AS z_cluster, 0 AS z_isMatch")
        updated_rows = [tuple(row) for row in zingg.update_label(pairs, explicit).collect()]
        assert updated_rows[0][-1] == 2, updated_rows
        labeled_values = ", ".join(
            f"('{prefix}{i}-{side}', '{prefix}{i}', {label})"
            for prefix, label in (("p", 1), ("n", 0))
            for i in range(5)
            for side in ("left", "right")
        )
        labeled_records = spark.sql(
            f"SELECT * FROM VALUES {labeled_values} AS t(record_id, z_cluster, z_isMatch)"
        )
        training_pairs = zingg.build_training_pairs(labeled_records, "record_id")
        training_evidence = zingg.inspect_training_evidence(training_pairs)
        assert training_evidence == {"positive_pairs": 5, "negative_pairs": 5, "sufficient": True}
        evidence = {
            "status": status,
            "similarities": {
                "jaccard": [tuple(row) for row in jaccard_rows],
                "jaro": [tuple(row) for row in jaro_rows],
            },
            "rows": rows,
            "labeled": labeled_rows,
            "updated": updated_rows,
            "trainingEvidence": training_evidence,
        }
        encoded = json.dumps(evidence, sort_keys=True)
        evidence_path = os.environ.get("ZINGG_NATIVE_EVIDENCE_PATH")
        if evidence_path:
            with open(evidence_path, "w", encoding="utf-8") as handle:
                handle.write(encoded + "\n")
        print(encoded)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
