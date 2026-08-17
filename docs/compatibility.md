# Compatibility

**Status: architecture remediation in progress — not release ready.**

| Runtime / transport | Exact | Jaccard | Jaro | Evidence |
|---|---:|---:|---:|---|
| Local Spark 4.0 Classic + Scala core JAR | PASS | PASS | PASS | Real Py4J execution; Jaro matches SecondString vectors |
| Databricks Dedicated + Photon | OUT OF SCOPE | OUT OF SCOPE | OUT OF SCOPE | Serverless-only project target |
| Databricks Serverless / managed Connect | NOT VERIFIED | NOT VERIFIED | NOT VERIFIED | Custom server plugin installation is not proven |
| Databricks Serverless JAR task / shared Scala core | PASS (Jaccard) | NOT TESTED | NOT TESTED | Real job `295665184144562`, Spark 4.1.0; plugin loading not proven |
| Self-managed Spark Connect 4.1 | PASS | PASS | OPEN | Exact and Jaccard executed through local Spark 4.1 plugin; Jaro remains open |
| Fabric Runtime 2 / Gluten+Velox | DEFERRED | DEFERRED | DEFERRED | Outside current scope |

The previous Serverless wheel-only runs are retained as prototype expression
feasibility evidence. They do not validate this repository's shared Scala core
or Connect server plugin and are not support evidence.
