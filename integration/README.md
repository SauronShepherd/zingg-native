# Zingg 0.7.0 integration overlay

`zingg-0.7.0-overlay/` contains the minimal Spark-specific source changes that make ordinary Zingg execution call `zingg-native-core` automatically. It does not fork Zingg phase orchestration. `SparkModel` is patched only at its Spark execution boundary so native mode can replace the non-Serverless Spark-ML estimator block transparently.

The Spark 4 semantic source is represented inside this repository by `reference/upstream-zingg` and the production overlay under `integration/zingg-0.7.0-overlay`. The upstream Zingg checkout is external, immutable, and is not modified or required as a working-tree dependency.

Patched boundaries are `SparkSimFunction`, `SparkBaseTransformer`, `SparkTransformer`, `SparkHashFunction`, `SparkHashUtil`, `SparkStopWordsRemover`, `VectorValueExtractor`, `SparkBlockingTreeUtil`, `SparkGraphUtil`, `SparkModel`, and `ZinggSparkContext`.

Apply with:

```text
The overlay is maintained in this repository. Package the native artifacts here and supply a separately obtained Zingg 0.7.0 Spark 4 assembly at Databricks deployment time.
```

The native project does not edit, patch, or build a Zingg checkout. The separately supplied Zingg assembly must be placed beside the native JAR and must pass `scripts/check-zingg-assembly.py`.

Native REWRITE/STRICT mode does not need GraphFrames. The original GraphFrames branch is retained reflectively only for OFF/AUDIT compatibility.


In native mode `SparkModel` writes `_zingg_native_model_v1` below the normal Zingg model path. Build with `-Plegacy-model-converter` and use `ai.zingg.native.tools.LegacyCrossValidatorModelConverter <model-path>` on Dedicated/local Spark to add this sidecar to an existing Zingg 0.7 model.
