# Compatibility

**Status: architecture remediation in progress — not release ready.**

| Runtime / transport | Exact | Jaccard | Jaro | Evidence |
|---|---:|---:|---:|---|
| Local Spark 4.0 Classic + Scala core JAR | PASS | PASS | PASS | Real Py4J execution; Jaro matches SecondString vectors |
| Databricks Dedicated + Photon | OUT OF SCOPE | OUT OF SCOPE | OUT OF SCOPE | Serverless-only project target |
| Databricks Serverless / managed Connect | UNSUPPORTED | UNSUPPORTED | UNSUPPORTED | Job `177009162619307` rejected plugin config with `CONFIG_NOT_AVAILABLE` |
| Databricks Serverless JAR task / shared Scala core | PASS (Exact + Jaccard + Jaro + preprocessing + 3 phases + UC-volume persistence) | NOT TESTED | NOT TESTED | Real job `295665184144562`, run `847651604040137`, Spark 4.1.0; plugin loading not proven |
| Databricks Serverless Asset Bundle / shared Scala core | PASS (similarities + preprocessing + phase subset + UC-volume persistence) | NOT TESTED | NOT TESTED | Bundle job `190949869955356`, run `168088194011842`, task `1072118149398301`, Spark 4.1.0; plugin loading not proven |
| Self-managed Spark Connect 4.1 | PASS | PASS | OPEN | Exact and Jaccard executed through local Spark 4.1 plugin; Jaro remains open |
| Fabric Runtime 2 / Gluten+Velox | DEFERRED | DEFERRED | DEFERRED | Outside current scope |

The previous Serverless wheel-only runs are retained as prototype expression
feasibility evidence. They do not validate this repository's shared Scala core
or Connect server plugin and are not support evidence.
