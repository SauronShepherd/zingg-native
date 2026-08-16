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
# Current transport boundary

The Classic/Py4J path now routes Exact, Jaccard, and Jaro through the shared
Scala core. The Spark Connect plugin currently routes Exact through the
versioned expression payload; Jaccard and Jaro remain pending Connect parity
verification. No Databricks Serverless support claim is made.
