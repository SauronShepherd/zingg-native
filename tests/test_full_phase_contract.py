from zingg_native import Zingg


def test_complete_native_phase_contract(spark):
    """Exercise every documented non-interactive phase on Spark expressions."""
    records = spark.createDataFrame(
        [("r1", "10"), ("r2", "10"), ("r3", "20"), ("r4", "30")],
        "record_id string, customer_key string",
    )
    zingg = Zingg(spark=spark, backend="expressions")
    pairs = zingg.execute(
        "findTrainingData", df=records, keys=["customer_key"],
        id_column="record_id", include_all_pairs=True,
    )
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
