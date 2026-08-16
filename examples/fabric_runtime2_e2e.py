# Fabric Runtime 2 notebook source. Run with Fabric Native Execution Engine enabled.

import json
from pyspark.sql import functions as F
from zingg_native import Zingg, detect_runtime

if not spark.version.startswith("4."):
    raise RuntimeError(f"Expected Fabric Runtime 2 / Spark 4.x, got {spark.version}")

zingg = Zingg(spark=spark)
input_df = spark.range(0, 4).select(
    F.when(F.col("id") == 0, "Alice").when(F.col("id") == 1, "Alice").otherwise(None).alias("left"),
    F.when(F.col("id") == 0, "Alice").when(F.col("id") == 1, "Bob").when(F.col("id") == 2, "Bob").otherwise(None).alias("right"),
)
exact_result = zingg.exact(input_df, "left", "right")
assert sorted(r.z_exact for r in exact_result.collect()) == [0.0, 1.0, 1.0, 1.0]

records = spark.createDataFrame(
    [("r1", "10"), ("r2", "10"), ("r3", "20"), ("r4", "30")],
    "record_id string, customer_key string",
)
pairs = zingg.find_training_data(records, ["customer_key"], "record_id", include_all_pairs=True)
labeled = zingg.label(pairs)
updated = zingg.update_label(labeled, labeled.select("z_cluster", "z_isMatch"))
model = zingg.train(updated, ["customer_key"])
clusters = zingg.match(records, model)
assert pairs.count() == 6
assert clusters.select("z_cluster").distinct().count() == 3

runtime = detect_runtime(spark)
exact_result.createOrReplaceTempView("zingg_native_fabric_exact_evidence")
plan = "\n".join(row[0] for row in spark.sql("EXPLAIN FORMATTED SELECT * FROM zingg_native_fabric_exact_evidence").collect())
native_markers = ("Velox", "Gluten", "Native", "Transformer")
evidence = {
    "spark_version": runtime.spark_version,
    "api_mode": runtime.api_mode,
    "operation": "EXACT_AND_PHASES",
    "phases": ["findTrainingData", "label", "updateLabel", "train", "match"],
    "rows_checked": 4,
    "native_plan_detected": any(marker.lower() in plan.lower() for marker in native_markers),
    "plan": plan,
    "status": "PASS",
}
dbutils.notebook.exit(json.dumps(evidence, sort_keys=True))
