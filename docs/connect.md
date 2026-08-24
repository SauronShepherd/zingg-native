# Spark Connect

Spark Connect is a transport boundary, not a second semantic implementation.

The Serverless path constructs the same public Spark SQL/DataFrame expressions as the Dedicated path. The managed runtime transports those plans. `zingg-native` does not require `ExpressionPlugin`, `SparkConnectPlanner`, SparkSession extensions, or custom protobuf messages in production.

The Python `ConnectBackend` is transport/control-only and does not duplicate semantic rewrites. Ordinary Zingg phases and all native semantics remain in the JVM `NativeOperationProvider`.

The abandoned custom planner implementation is archived under `reference/legacy-connect-plugin/` for provenance. It is not part of the Maven reactor and must not be included in a production deployment.
