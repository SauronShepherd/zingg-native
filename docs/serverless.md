# Serverless implementation

The Serverless production reactor is activated with `-Pdatabricks-serverless-env5`. `core` compiles against the provided Databricks Connect API instead of packaging Apache Spark runtime artifacts. `serverless-launcher` supplies `ai.zingg.native.launch.DatabricksZinggMain`.

The launcher accepts `--delegate-main <class>`, `--native-mode`, `--native-run-id`, `--native-disabled-rules`, and `--native-plan-guard`. It creates the managed `DatabricksSession`, sets it as Spark's active/default session, translates adapter flags to JVM properties, and invokes the real Zingg main with all remaining arguments unchanged.

There is no production custom Connect server plugin, Catalyst rule, or alternate Zingg phase implementation.

Serverless mode contract: `REWRITE` and `STRICT` are supported. `STRICT` is the
production default and fails closed for unknown or disabled operations. `OFF`
and `AUDIT` are rejected by the launcher before ordinary Zingg execution;
those modes belong to the classic/Dedicated artifact, where the legacy
GraphFrames and Spark-ML compatibility path is available. This prevents a
Serverless job from accepting a legacy mode and failing later inside model or
graph execution.

The REWRITE/STRICT Serverless branch does not call `cache`, `persist`, `unpersist`, or `checkpoint`. Model fitting does not invoke Scala `LogisticRegression.fit`/`CrossValidator.fit`; `NativeModelEngine` expresses training with public DataFrame operations and reads/writes the versioned native sidecar.
