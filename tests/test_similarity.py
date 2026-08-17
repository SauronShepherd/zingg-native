from zingg_native import Zingg
from zingg_native.similarity import jaccard_similarity, jaro_similarity


def test_jaccard_matches_upstream_token_semantics(spark):
    rows = [
        ("New York", "new-york", 1.0),
        ("new york", "new jersey", 1.0 / 3.0),
        (None, "anything", 1.0),
        ("", "anything", 1.0),
    ]
    df = spark.sql("""
        SELECT 'New York' AS left, 'new-york' AS right, 1.0D AS expected
        UNION ALL SELECT 'new york', 'new jersey', 1.0D / 3.0D
        UNION ALL SELECT CAST(NULL AS STRING), 'anything', 1.0D
        UNION ALL SELECT '', 'anything', 1.0D
    """)
    actual = [r[0] for r in df.select(jaccard_similarity("left", "right")).collect()]
    expected = [r[2] for r in rows]
    assert all(abs(a - e) < 1e-12 for a, e in zip(actual, expected, strict=True))


def test_jaccard_is_exposed_through_native_facade(spark):
    df = spark.sql("SELECT 'New York' AS left, 'new-york' AS right")
    # This test exercises the preserved formula prototype explicitly. The
    # production facade defaults to the shared Scala Classic transport.
    actual = Zingg(spark=spark, backend="expressions").jaccard(df, "left", "right").first().z_jaccard
    assert actual == 1.0


def test_jaro_matches_secondstring_oracle_vectors(spark):
    df = spark.sql("""
        SELECT 'MARTHA' AS left, 'MARHTA' AS right, 0.9444444444444445D AS expected
        UNION ALL SELECT 'DWAYNE', 'DUANE', 0.8222222222222223D
        UNION ALL SELECT 'CRATE', 'TRACE', 0.8666666666666667D
        UNION ALL SELECT 'abc', 'xyz', 0.0D
    """)
    actual = [r[0] for r in df.select(jaro_similarity("left", "right")).collect()]
    expected = [r[2] for r in df.collect()]
    assert all(abs(a - e) < 1e-12 for a, e in zip(actual, expected, strict=True))
