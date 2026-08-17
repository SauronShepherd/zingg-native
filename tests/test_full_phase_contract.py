from zingg_native import Zingg


def test_complete_native_phase_contract(spark):
    """Exercise every documented non-interactive phase on Spark expressions."""
    records = spark.createDataFrame(
        [("r1", "10"), ("r2", "10"), ("r3", "20"), ("r4", "30")],
        "record_id string, customer_key string",
    )
    zingg = Zingg(spark=spark, backend="expressions")
    candidate_schema = """z_cluster string, z_left_record_id string,
        z_right_record_id string, z_score double, z_isMatch int"""
    pairs = spark.createDataFrame(
        [("p1", "r1", "r2", 1.0, None), ("p2", "r1", "r3", 0.0, None)],
        candidate_schema,
    )
    # Candidate generation is covered as a plan/schema contract here; the
    # full Cartesian action is exercised by the Databricks E2E job.
    assert set(zingg.find_training_data(records, ["customer_key"], "record_id").columns) >= {
        "z_cluster", "z_left_record_id", "z_right_record_id", "z_score"
    }
    labeled = zingg.execute("label", pairs=pairs, match_threshold=1.0)
    updated = zingg.execute(
        "updateLabel", pairs=labeled,
        labels=labeled.select("z_cluster", "z_isMatch"),
    )
    model = zingg.execute("train", labeled=updated, keys=["customer_key"])
    matched = zingg.execute("match", df=records, model=model)
    linked = zingg.execute("link", df=records, model=model)
    docs = zingg.execute("generateDocs", model=model)

    assert matched.select("z_cluster").distinct().count() == 3
    assert linked.count() == 4
    assert docs["model"]["algorithm"] == "EXACT_KEYS"
