# Similarity feasibility and parity status

The semantic oracle is the Zingg 0.7.0 source and jar in the sibling
`zingg` checkout. The native implementation must preserve the algorithm; a
different approximate metric is not an acceptable fallback.

| Upstream function | Reference implementation | Native status | Evidence |
|---|---|---|---|
| Exact | null-or-equal score | Implemented in shared Scala core | Local Classic/Py4J execution and semantic tests; Databricks not verified |
| Jaccard | SecondString `Jaccard` + `SimpleTokenizer` | Implemented in shared Scala core | Local Classic/Py4J execution and Unicode parity tests; Databricks not verified |
| Jaro | SecondString `Jaro` implementation used by Zingg 0.7 | Implemented as native Spark SQL expression; Photon fallback | Local Classic semantic vectors; Photon/Databricks execution not verified |
| Affine Gap | SecondString `SAffineGap` / Monge-Elkan | Pending feasibility spike | Upstream source inventoried; no equivalent native expression claimed |

Affine Gap is intentionally not exposed as an implemented native function.
Registering it as an alias for Levenshtein, Jaro, or another approximate score
would violate the Zingg semantic contract. Jaro is exposed as Spark SQL, but
its potential Photon fallback is explicit because the required higher-order
aggregate has not been verified on the target Photon runtime.

## Extracted 0.7 oracle vectors

These values were executed from `zingg-0.7.0.jar` using SecondString's
`Jaro` and Zingg's `SAffineGap` classes:

`SAffineGap` is not merely a single scalar edit-distance formula: it inherits
SecondString `MongeElkan`, which uses a three-state affine-gap dynamic
programming matrix, `open=-5`, `extend=-1`, approximate character classes,
and a scaling factor of `min(length(left), length(right)) / 5`. A faithful
Spark implementation would need to materialize and update that per-row matrix;
the current Spark/Photon expression surface has no recursive matrix operator,
so no approximate replacement is claimed.

| left | right | Jaro | Affine Gap |
|---|---|---:|---:|
| `MARTHA` | `MARHTA` | 0.9444444444444445 | 0.5 |
| `DWAYNE` | `DUANE` | 0.8222222222222223 | 0.48 |
| `CRATE` | `TRACE` | 0.8666666666666667 | 0.48 |
| `abc` | `xyz` | 0.0 | 0.0 |
| `prefix` | `prefixation` | 0.8484848484848485 | 1.0 |
# Shared-core migration status

Exact, Jaccard, and Jaro are now implemented in the Scala 2.13 core and
exposed through the Classic gateway. Jaro is semantically certified but uses
Spark higher-order expressions and therefore must be reported as Photon
fallback unless a target runtime demonstrates native execution.
