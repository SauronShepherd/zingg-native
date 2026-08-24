# Zingg Native working rules

- `reference/upstream-zingg` is the pinned Zingg 0.7.0 semantic reference. Do not edit it; production integration belongs in `integration/zingg-0.7.0-overlay`.
- Supported runtime family is Spark 4.x with Scala 2.13 and Java 17. Do not add Spark 3 or Scala 2.12 compatibility shims to the Databricks production artifact.
- Product scope is **Databricks Dedicated + Photon and Databricks Serverless**. Both are mandatory; neither may be documented as out of scope.
- Keep the product simple: ordinary Zingg constructs the job; the adapter intercepts known Spark-specific operation boundaries, replaces non-native constructs with equivalent public Spark expressions, and lets Spark execute the resulting plan.
- Do not create a parallel entity-resolution engine or require users to rewrite normal Zingg jobs to a `zingg_native` API.
- Upstream Zingg 0.7.0 behavior is the semantic source of truth. Rewrites change execution representation only.
- Production rewrites must use public `Column`, `Dataset<Row>`, DataFrame, Spark SQL, and public Spark ML functions. No Python UDF, Scala UDF, Catalyst API, planner extension, SparkSessionExtension, or SparkContext dependency in the Serverless/common artifact.
- Classic/Py4J and managed Spark Connect execution are transport variants over the same rewrite registry, not separate semantic implementations.
- The historical custom Spark Connect expression plugin is archived under `reference/legacy-connect-plugin/` and must never be reintroduced as a production or Serverless dependency.
- `STRICT` mode fails closed for unknown/unmapped native operations. Never silently fall back while claiming native execution.
- Actual Photon execution is an evidence question, not a configuration inference. Do not call a path Photon-native until real Databricks runtime/query-profile evidence proves it.
- Every rewrite must be observable and individually disable-able through the rule registry/evidence system.
