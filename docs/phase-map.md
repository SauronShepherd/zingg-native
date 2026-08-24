# Zingg phase interception map

The adapter does not reimplement phases. It patches reusable Spark construction points reached by multiple phases.

| Choke point | Original Spark construct | Native replacement |
|---|---|---|
| `SparkBaseTransformer` | similarity `callUDF` | registered public Column expression |
| `SparkTransformer` | similarity UDF registration | bypass in rewrite modes |
| `SparkHashFunction` | blocking `callUDF` | registered public Column expression |
| `SparkHashUtil` | hash UDF registration | bypass in rewrite modes |
| `SparkStopWordsRemover` | stop-word UDF | `regexp_replace` expression |
| `SparkBlockingTreeUtil` | typed `Dataset.map` | driver-side tree-to-IR compiler + Column expression |
| `VectorValueExtractor` | ML vector UDF | public `Column` extraction from the VectorUDT `values` struct |
| `SparkModel` | VectorAssembler + PolynomialExpansion + LogisticRegression + CrossValidator | public DataFrame polynomial/logistic-CV engine + versioned model sidecar |
| `SparkGraphUtil` | GraphFrames connected components | relational join/group/min fixed-point algorithm |
| `ZinggSparkContext` | `JavaSparkContext` lifecycle access | managed `SparkSession` only |

Because these are shared internal boundaries, normal `findTrainingData`, labeling, training, matching, and linking continue to be orchestrated by upstream Zingg while receiving the rewritten Spark operations.
