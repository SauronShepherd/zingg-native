# Semantic contracts

## EXACT_SIMILARITY

- Reference: Zingg 0.7.0 `SimilarityFunctionExact.call`.
- Inputs: two values of the same Spark-compatible type.
- If either input is null, return `1.0`.
- Otherwise return `1.0` when the values are equal and `0.0` otherwise.
- Implementation: `CASE` expressions only; no Python or Scala UDF.
