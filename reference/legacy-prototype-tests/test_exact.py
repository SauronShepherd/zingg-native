from zingg_native.similarity import exact_similarity


def test_exact_expression_has_zingg_null_semantics(spark):
    df = spark.sql("""
        SELECT CAST(NULL AS STRING) AS left, CAST(NULL AS STRING) AS right
        UNION ALL SELECT CAST(NULL AS STRING), 'x'
        UNION ALL SELECT 'x', 'x'
        UNION ALL SELECT 'x', 'y'
    """)
    actual = [r[0] for r in df.select(exact_similarity("left", "right")).collect()]
    assert actual == [1.0, 1.0, 1.0, 0.0]
