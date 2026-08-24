# Compatibility matrix

| Target | Production path | Source status |
|---|---|---|
| Databricks Dedicated + Photon | patched Zingg JVM + shared public-expression core | implemented-unvalidated |
| Databricks Serverless | DatabricksSession bootstrap + patched Zingg + same core | runtime-validated; Photon participation evidenced, operator attribution unverified |
| Spark 4.0 / Scala 2.13 | `spark40` / Dedicated 17.3 profile | implemented-unvalidated |
| Spark 4.1 / Scala 2.13 | `spark41` / Dedicated 18 profile | implemented-unvalidated |
| custom Connect planner plugin | archived reference only | not a production dependency |

The semantic baseline is pinned Zingg 0.7.0. Databricks-only runtime validation
has covered Serverless `findTrainingData`, bounded and full-shape native
training, native persistence, cross-job reload, a strict fail-closed negative
case, and Photon participation via nonzero `photon_total_time_ms` on
job-linked queries. Semantic parity, operator-level Photon attribution, and
Dedicated execution remain unvalidated because the current workspace rejects
Dedicated compute.
