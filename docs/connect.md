# Spark Connect

The Connect artifact is compiled against Spark 4.1.0 and exposes
`ZinggNativeExpressionPlugin` through Spark's `ExpressionPlugin` API. The
Python backend serializes a version-1 Exact payload with two child Spark
expressions; the server plugin parses it and delegates those children to
`SparkConnectPlanner` before constructing the shared Catalyst expression.

For self-managed Spark Connect, build the artifacts and run
`scripts/start-connect-server.ps1` with `SPARK_HOME` pointing at a Spark 4.1
distribution. Configure the same plugin class through
`spark.connect.extensions.expression.classes`.

Databricks Serverless and Databricks Connect are not claimed until the server
plugin can be installed on the actual managed server and the same payload is
executed there.
