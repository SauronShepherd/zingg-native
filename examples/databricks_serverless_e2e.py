# Databricks notebook source

# COMMAND ----------

import json
from pyspark.sql import functions as F
from zingg_native import Zingg, detect_runtime, exact_similarity, jaro_similarity

if not spark.version.startswith("4."):
    raise RuntimeError(
        f"zingg-native requires Spark 4.x; this Databricks environment reports {spark.version}. "
        "Select a Spark 4 Databricks Runtime before running the adapter."
    )

input_df = (spark.range(0, 4)
            .select(
                F.when(F.col("id") == 0, "Alice").when(F.col("id") == 1, "Alice").otherwise(None).alias("left"),
                F.when(F.col("id") == 0, "Alice").when(F.col("id") == 1, "Bob").when(F.col("id") == 2, "Bob").otherwise(None).alias("right"),
            ))

zingg = Zingg(spark=spark)
result = zingg.exact(input_df, "left", "right", "z_exact")
actual = [row.z_exact for row in result.orderBy(F.col("left").asc_nulls_last(), F.col("right").asc_nulls_last()).collect()]
assert sorted(actual) == [0.0, 1.0, 1.0, 1.0], actual

records = spark.createDataFrame(
    [("r1", "10"), ("r2", "10"), ("r3", "20"), ("r4", "30")],
    "record_id string, customer_key string",
)
training_pairs = zingg.execute("findTrainingData", df=records, keys=["customer_key"], id_column="record_id", include_all_pairs=True)
assert training_pairs.count() == 6
labeled = zingg.execute("label", pairs=training_pairs, match_threshold=1.0)
updated = zingg.execute("updateLabel", pairs=labeled, labels=labeled.select("z_cluster", "z_isMatch"))
model = zingg.execute("train", labeled=updated, keys=["customer_key"])
clusters = zingg.execute("match", df=records, model=model)
linked = zingg.execute("link", df=records, model=model)
docs = zingg.execute("generateDocs", model=model)
assert clusters.groupBy("z_cluster").count().where("count > 1").count() == 1
assert clusters.select("z_cluster").distinct().count() == 3
assert linked.count() == records.count()
assert docs["model"]["algorithm"] == "EXACT_KEYS"

# Complete non-interactive fuzzy feature/training path: candidate generation,
# native Exact/Jaro feature construction, score aggregation, labeling, and
# threshold-model training. This is deliberately separate from the exact-key
# cluster shortcut above so the feature contract is exercised directly.
fuzzy = spark.createDataFrame(
    [("f1", "Alice Smith", "alice@example.com"),
     ("f2", "Alice Smyth", "alice@example.com"),
     ("f3", "Bob Jones", "bob@example.com")],
    "record_id string, name string, email string",
)
fl = fuzzy.alias("fl")
fr = fuzzy.alias("fr")
fuzzy_pairs = (fl.crossJoin(fr)
    .where(F.col("fl.record_id") < F.col("fr.record_id"))
    .select(
        F.sha2(F.concat_ws("|", F.col("fl.record_id"), F.col("fr.record_id")), 256).alias("z_cluster"),
        F.col("fl.record_id").alias("z_left_record_id"),
        F.col("fr.record_id").alias("z_right_record_id"),
        exact_similarity(F.col("fl.email"), F.col("fr.email")).alias("z_exact"),
        jaro_similarity(F.col("fl.name"), F.col("fr.name")).alias("z_jaro"),
    ))
fuzzy_scored = zingg.score_features(fuzzy_pairs)
fuzzy_labeled = (fuzzy_scored
    .withColumn("z_isMatch", F.when(F.col("z_exact") == 1.0, 1).otherwise(0)))
fuzzy_model = zingg.train(fuzzy_labeled, ["name", "email"], match_threshold=0.8,
                          feature_functions={"name": "JARO", "email": "EXACT"})
fuzzy_matches = zingg.match_pairs(fuzzy_labeled, fuzzy_model)
fuzzy_links = zingg.link_pairs(fuzzy_labeled, fuzzy_model)
fuzzy_direct_matches = zingg.match(fuzzy, fuzzy_model)
fuzzy_direct_links = zingg.link(fuzzy, fuzzy_model)
fuzzy_clusters = zingg.cluster_pairs(fuzzy_direct_matches)
left_source = fuzzy.where(F.col("record_id").isin("f1", "f3"))
right_source = fuzzy.where(F.col("record_id").isin("f2")).unionByName(
    spark.createDataFrame([("f4", "Bob Jones", "bob@example.com")], "record_id string, name string, email string")
)
source_links = zingg.link_sources(left_source, right_source, fuzzy_model)
assert fuzzy_labeled.count() == 3
assert fuzzy_labeled.where("z_isMatch = 1").count() == 1
assert fuzzy_labeled.where("z_isMatch = 0").count() == 2
assert fuzzy_model["positive_pairs"] == 1 and fuzzy_model["negative_pairs"] == 2
assert fuzzy_model["algorithm"] == "NATIVE_FEATURE_THRESHOLD"
assert fuzzy_matches.count() == 1 and fuzzy_links.count() == 1
assert fuzzy_direct_matches.count() == 1 and fuzzy_direct_links.count() == 1
assert fuzzy_clusters.count() == 2
assert fuzzy_clusters.select("z_cluster").distinct().count() == 1
assert source_links.count() == 2

jaccard_input = (spark.range(0, 4).select(
    F.when(F.col("id") == 0, "New York")
     .when(F.col("id") == 1, "new york")
     .when(F.col("id") == 3, "")
     .otherwise(None).alias("left"),
    F.when(F.col("id") == 0, "new-york")
     .when(F.col("id") == 1, "new jersey")
     .otherwise("x").alias("right"),
))
jaccard_result = zingg.jaccard(jaccard_input, "left", "right")
jaccard_scores = [r[0] for r in jaccard_result.select("z_jaccard").collect()]
assert jaccard_scores == [1.0, 1.0 / 3.0, 1.0, 1.0], jaccard_scores

jaro_input = spark.range(0, 6).select(
    F.when(F.col("id") == 0, "MARTHA")
     .when(F.col("id") == 1, "DWAYNE")
     .when(F.col("id") == 2, "CRATE")
     .when(F.col("id") == 3, "abc")
     .when(F.col("id") == 5, "")
     .otherwise(None).alias("left"),
    F.when(F.col("id") == 0, "MARHTA")
     .when(F.col("id") == 1, "DUANE")
     .when(F.col("id") == 2, "TRACE")
     .when(F.col("id") == 3, "xyz")
     .otherwise("x").alias("right"),
)
jaro_scores = [r[0] for r in zingg.jaro(jaro_input, "left", "right").select("z_jaro").collect()]
for actual_score, expected_score in zip(jaro_scores, [0.9444444444444445, 0.8222222222222223, 0.8666666666666667, 0.0, 1.0, 1.0]):
    assert abs(actual_score - expected_score) < 1e-12, (actual_score, expected_score)

runtime = detect_runtime(spark)
def safe_conf(key):
    try:
        return spark.conf.get(key, "unknown")
    except Exception:
        return "unavailable"

evidence = {
    "spark_version": runtime.spark_version,
    "api_mode": runtime.api_mode,
    "engine": runtime.engine,
    "native_execution_detected": runtime.native_execution,
    "serverless_environment": safe_conf("spark.databricks.clusterUsageTags.sparkVersion"),
    "photon_enabled": safe_conf("spark.databricks.photon.enabled"),
    "operation": "EXACT_SIMILARITY",
    "rows_checked": len(actual),
    "exact_clusters": clusters.select("z_cluster").distinct().count(),
    "phases": ["findTrainingData", "label", "updateLabel", "train", "match", "link", "generateDocs"],
    "fuzzy_pipeline": {
        "candidate_pairs": fuzzy_labeled.count(),
        "feature_columns": ["z_exact", "z_jaro"],
        "positive_pairs": fuzzy_model["positive_pairs"],
        "negative_pairs": fuzzy_model["negative_pairs"],
        "matched_pairs": fuzzy_matches.count(),
        "linked_pairs": fuzzy_links.count(),
        "direct_match_pairs": fuzzy_direct_matches.count(),
        "direct_link_pairs": fuzzy_direct_links.count(),
        "clustered_records": fuzzy_clusters.count(),
        "clusters": fuzzy_clusters.select("z_cluster").distinct().count(),
        "cross_source_links": source_links.count(),
        "status": "PASS",
    },
    "similarity_parity": {"JACCARD": "PASS"},
    "status": "PASS",
}
try:
    result.createOrReplaceTempView("zingg_native_exact_evidence")
    evidence["exact_plan"] = "\n".join(row[0] for row in spark.sql("EXPLAIN FORMATTED SELECT * FROM zingg_native_exact_evidence").collect())
    evidence["native_execution_detected"] = "Photon" in evidence["exact_plan"] and "fully supported by Photon" in evidence["exact_plan"]
    jaccard_result.createOrReplaceTempView("zingg_native_jaccard_evidence")
    evidence["jaccard_plan"] = "\n".join(row[0] for row in spark.sql("EXPLAIN FORMATTED SELECT * FROM zingg_native_jaccard_evidence").collect())
    evidence["jaccard_native_execution_detected"] = "Photon" in evidence["jaccard_plan"] and "fully supported by Photon" in evidence["jaccard_plan"]
    jaro_input.createOrReplaceTempView("zingg_native_jaro_evidence")
    jaro_plan_df = zingg.jaro(jaro_input, "left", "right")
    jaro_plan_df.createOrReplaceTempView("zingg_native_jaro_evidence")
    evidence["jaro_plan"] = "\n".join(row[0] for row in spark.sql("EXPLAIN FORMATTED SELECT * FROM zingg_native_jaro_evidence").collect())
    evidence["jaro_native_execution_detected"] = (
        "Photon" in evidence["jaro_plan"]
        and "fully supported by Photon" in evidence["jaro_plan"]
        and "does not fully support" not in evidence["jaro_plan"]
    )
    fuzzy_direct_matches.createOrReplaceTempView("zingg_native_fuzzy_evidence")
    evidence["fuzzy_plan"] = "\n".join(row[0] for row in spark.sql("EXPLAIN FORMATTED SELECT * FROM zingg_native_fuzzy_evidence").collect())
    evidence["fuzzy_native_execution_detected"] = (
        "Photon" in evidence["fuzzy_plan"]
        and "fully supported by Photon" in evidence["fuzzy_plan"]
        and "does not fully support" not in evidence["fuzzy_plan"]
    )
except Exception as exc:
    evidence["exact_plan"] = f"EXPLAIN unavailable: {type(exc).__name__}: {exc}"
dbutils.notebook.exit(json.dumps(evidence, sort_keys=True))
