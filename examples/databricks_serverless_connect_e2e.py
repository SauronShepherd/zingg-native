# Databricks notebook source
"""Real Serverless managed-Connect feasibility test for the native plugin."""

from zingg_native import Zingg

spark.conf.set("zingg.native.connect.plugin.loaded", "true")
z = Zingg(spark=spark, backend="connect")

exact = z.exact(
    spark.sql("SELECT 'x' AS left_value, 'x' AS right_value UNION ALL SELECT 'x', 'y'"),
    "left_value", "right_value",
).collect()
assert [row[2] for row in exact] == [1.0, 0.0], exact

jaccard = z.jaccard(
    spark.sql("SELECT 'New York' AS left_value, 'new-york' AS right_value"),
    "left_value", "right_value",
).collect()
assert jaccard[0][2] == 1.0, jaccard

print({"status": z.status(), "exact": [tuple(row) for row in exact],
       "jaccard": [tuple(row) for row in jaccard]})
