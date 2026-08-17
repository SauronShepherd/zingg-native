# Spark Connect

The Connect artifact is compiled against Spark 4.1.0 and exposes
`ZinggNativeExpressionPlugin` through Spark's `ExpressionPlugin` API. The
Python backend serializes version-1 Exact and Jaccard payloads with two child
Spark expressions; the server plugin parses those children through
`SparkConnectPlanner` and constructs standard Catalyst expressions. Jaro is
still pending a Catalyst-native Connect expression implementation.

Local Spark 4.1 Connect E2E has been verified for Exact and Jaccard through the
installed plugin (WSL-hosted server, Windows client). The checks covered
`New York` versus `new-york` (`1.0`) and `x/x`, `x/y` (`1.0`, `0.0`). Jaro
Connect E2E remains open.

For self-managed Spark Connect, build the artifacts and run
`scripts/start-connect-server.ps1` with `SPARK_HOME` pointing at a Spark 4.1
distribution. Configure the same plugin class through
`spark.connect.extensions.expression.classes`.

With the server listening on port 15002, run
`python examples/connect_similarity_e2e.py` to reproduce the verified Exact
and Jaccard transport checks.

On Windows, configure `HADOOP_HOME` with `bin\winutils.exe` before running the
launcher. The script checks this prerequisite explicitly; Linux/WSL hosts do
not require that Windows utility.

Databricks Serverless and Databricks Connect are not claimed until the server
plugin can be installed on the actual managed server and the same payload is
executed there.
