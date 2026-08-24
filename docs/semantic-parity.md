# Semantic parity design

The pinned `reference/upstream-zingg` 0.7.0 source is the semantic oracle. Native rules intentionally reproduce upstream edge behavior rather than replacing algorithms with approximate alternatives.

Important implementation choices include Java UTF-16 code-unit handling for SecondString/blocking character logic, Java-compatible string hashing and numeric rounding/overflow behavior, Zingg null/empty conventions, the original token/regex shapes, the original date ratio, the original range return types, and the SecondString affine-gap/Jaro algorithms expressed with higher-order Spark SQL operations.

The wrapper keeps Zingg phase orchestration and similarity feature definitions unchanged. The upstream Spark-ML model boundary is itself hostile to the mandatory Serverless Scala path, so native mode substitutes a versioned public-DataFrame logistic/CV implementation and sidecar persistence while preserving Zingg feature/column contracts. Legacy Spark-ML artifacts remain available in OFF/AUDIT and can be converted for native prediction.

The Serverless launcher includes differential probes that run the pinned Zingg
reference classes and native public-expression rules in the same Databricks
Serverless task. Current runtime evidence covers the registered similarity
families over bounded null/empty, mixed-case, Unicode, surrogate-pair, token,
and code fixtures, all 29 registered hash rules, every supported first/last-character
and last-word blocking family with candidate sets, and
the opaque row-ID contract. A typed numeric probe additionally covers integer,
long, float, and double boundaries including NaN/infinity fixtures. Full
phase-level intermediate-output parity and exhaustive model-oracle parity remain
release gates; the evidence file records those limits rather than promoting
probes into a broader claim. Additional Serverless probes cover the ordinary
SparkDFReader CSV/JSON/Parquet matrix, isolated Jaro and affine-gap actions,
and dense/sparse/null VectorUDT extraction under Spark 4 ANSI array bounds.
