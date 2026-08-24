# Architecture

`zingg-native` is a thin execution adapter around the real Zingg 0.7.0 Spark implementation. It does not replace Zingg phases, candidate generation, labeling, or user-facing model orchestration. It does replace Spark-hostile execution boundaries inside the model stage and writes a versioned native model sidecar in native mode.

```text
real Zingg operation
  -> patched Spark choke point
  -> NativeOperationProvider
  -> semantic RewriteRegistry
  -> public Spark SQL/DataFrame expression
  -> Spark planner/runtime
  -> STRICT plan guard + evidence hooks
```

The overlay intercepts before the original UDF, typed `Dataset.map`, GraphFrames-specific operation, or Spark-ML estimator training boundary is emitted. Therefore the production implementation does not need to mutate Catalyst plans. The semantic operation is known at the Zingg boundary and is rebuilt directly with public Spark expressions.

`OFF` preserves upstream execution, `AUDIT` records legacy constructs without rewriting, `REWRITE` applies known rules, and `STRICT` applies rules and rejects unknown/disabled operations or forbidden callback/object-encoder plan nodes. `STRICT` is the production default.

Dedicated and Serverless share the exact same registry. Dedicated runs the patched Zingg JVM application directly. Serverless uses a small `DatabricksSession` bootstrap and then delegates to the real Zingg main class. The historical custom Spark Connect planner plugin is archived under `reference/legacy-connect-plugin/` and is not a production dependency.


Model training follows the same rule: upstream Zingg creates the similarity features, then `SparkModel` delegates the assembler/polynomial/logistic-CV boundary to `NativeModelEngine`, which uses public DataFrame expressions/aggregates and versioned persistence. Existing legacy models can be converted on Dedicated/local Spark without changing Zingg application code.
