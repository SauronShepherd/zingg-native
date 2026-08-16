# Databricks support boundary

The supported architecture target is Databricks Runtime 18 LTS / Spark 4.1 /
Scala 2.13 on Dedicated compute with Photon, where a compute-scoped core JAR
can be loaded into the driver JVM and the Classic/Py4J gateway can be used.

Databricks Serverless remains unclaimed. The previous wheel-only Serverless
runs exercised Python-built Spark expressions through Spark Connect; they did
not install or execute this repository's Connect server plugin and therefore
cannot prove the shared-core architecture.

Required evidence before a Databricks release claim:

- core and Connect JARs installed on the target compute;
- gateway/plugin handshake reports protocol v1 and the expected core version;
- the same Exact/Jaccard outputs are observed through the claimed transport;
- Photon plan evidence is captured for each operation claimed native;
- unsupported Serverless/plugin deployment is recorded rather than inferred.
