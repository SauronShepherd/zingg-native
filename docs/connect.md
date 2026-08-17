# Spark Connect

The Connect artifact is compiled against Spark 4.1.0 and exposes
`ZinggNativeExpressionPlugin` through Spark's `ExpressionPlugin` API. The
Python backend serializes version-1 Exact, Jaccard, TRIM, and CASE_NORMALIZE
payloads; the server plugin parses those children through
`SparkConnectPlanner` and constructs standard Catalyst expressions. Jaro is
still pending a Catalyst-native Connect expression implementation.

Local Spark 4.1 Connect E2E has been verified for Exact, Jaccard, TRIM, and
CASE_NORMALIZE through the installed plugin (WSL-hosted server, Windows
client). The checks cover `New York` versus `new-york` (`1.0`), `x/x`, `x/y`
(`1.0`, `0.0`), and string preprocessing. Jaro Connect remains open.

For self-managed Spark Connect, build the artifacts and run
`scripts/start-connect-server.ps1` with `SPARK_HOME` pointing at a Spark 4.1
distribution. Configure the same plugin class through
`spark.connect.extensions.expression.classes`.

With the server listening on port 15002, run
`python examples/connect_similarity_e2e.py` to reproduce the verified Exact,
Jaccard, and preprocessing transport checks.

Install the client-side prerequisites separately with `pip install
"zingg-native[connect]"` in the Spark 4.1 Connect environment. Keep this
environment separate from the Classic `pyspark` test environment.

On Windows, configure `HADOOP_HOME` with `bin\winutils.exe` before running the
launcher. The script checks this prerequisite explicitly; Linux/WSL hosts do
not require that Windows utility.

For the verified WSL setup, the server was launched with Spark 4.1's
`org.apache.spark.deploy.SparkSubmit` and:

```text
--master local[2]
--class org.apache.spark.sql.connect.service.SparkConnectServer
--conf spark.connect.grpc.binding.port=15002
--conf spark.connect.extensions.expression.classes=ai.zingg.native.connect.ZinggNativeExpressionPlugin
--conf spark.jars=<core-jar>,<connect-jar>
```

On Java 21, add
`--add-opens=java.base/java.nio=ALL-UNNAMED` for Arrow result serialization.

Databricks Serverless managed Connect is currently unsupported on the tested
path: the real feasibility job was rejected with
`CONFIG_NOT_AVAILABLE` when setting the plugin activation configuration.
Self-managed Spark Connect remains the reproducible plugin deployment path.

The protocol also defines versioned `ModelArtifact` and
`BlockingTreeArtifact` references. They are compatibility envelopes only; the
native trainer and matcher do not yet consume or produce them.
