# Spark Connect

The Connect artifact is compiled against Spark 4.1.0 and exposes
`ZinggNativeExpressionPlugin` through Spark's `ExpressionPlugin` API. The
Python backend serializes version-1 Exact and Jaccard payloads with two child
Spark expressions; the server plugin parses those children through
`SparkConnectPlanner` and constructs standard Catalyst expressions. Jaro is
still pending a Catalyst-native Connect expression implementation.

For self-managed Spark Connect, build the artifacts and run
`scripts/start-connect-server.ps1` with `SPARK_HOME` pointing at a Spark 4.1
distribution. Configure the same plugin class through
`spark.connect.extensions.expression.classes`.

On Windows, configure `HADOOP_HOME` with `bin\winutils.exe` before running the
launcher. The script checks this prerequisite explicitly; Linux/WSL hosts do
not require that Windows utility.

Databricks Serverless and Databricks Connect are not claimed until the server
plugin can be installed on the actual managed server and the same payload is
executed there.
