# Compatibility

**Status: architecture remediation in progress — not release ready.**

| Runtime / transport | Exact | Jaccard | Jaro | Evidence |
|---|---:|---:|---:|---|
| Local Spark 4.0 Classic + Scala core JAR | PASS | PASS | PASS | Real Py4J execution; Jaro matches SecondString vectors |
| Databricks Dedicated + Photon | NOT VERIFIED | NOT VERIFIED | NOT VERIFIED | No worker environment is available in the `sda` workspace |
| Databricks Serverless / managed Connect | NOT SUPPORTED | NOT SUPPORTED | NOT SUPPORTED | Custom server plugin installation is not proven |
| Self-managed Spark Connect 4.1 | PROTOCOL BUILD ONLY | PROTOCOL BUILD ONLY | OPEN | Exact/Jaccard plugin payloads compile; server E2E pending |
| Fabric Runtime 2 / Gluten+Velox | DEFERRED | DEFERRED | DEFERRED | Outside current scope |

The previous Serverless wheel-only runs are retained as prototype expression
feasibility evidence. They do not validate this repository's shared Scala core
or Connect server plugin and are not support evidence.
