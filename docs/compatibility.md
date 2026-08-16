# Compatibility

| Runtime | Classic | Connect | Native engine |
|---|---|---|---|
| Apache Spark 4.0/4.1 | expression path | expression path | Spark-dependent |
| Databricks Photon | validated for Exact and Jaccard E2E | Serverless environment 4, Spark 4.1.0, Spark Connect; both expression plans report PhotonRange/PhotonProject and “fully supported by Photon” | 2026-08-16 |
| Fabric Runtime 2 / Gluten+Velox | deferred | deferred | deferred |

This validates Photon for the native Exact and Jaccard paths and the complete
implemented exact phase flow. Jaro is semantically validated as
Spark SQL but currently falls back from Photon because its required nested
`aggregate` expression is unsupported by the observed Serverless Photon
planner. It does not certify Affine Gap or every non-exact production workload.
