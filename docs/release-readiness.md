# Release readiness

Status: **Databricks scope ready for the supported native slice; Fabric deferred**.

## Verified gates

| Gate | Result | Evidence |
|---|---|---|
| Spark 4.1 Serverless environment 4 | PASS | Databricks run `1022375059248846` |
| Spark Connect facade | PASS | Same run reports `api_mode=connect` |
| Exact semantics | PASS | Null/equal/non-equal cases and Photon plan |
| Jaccard semantics | PASS | Token parity cases and Photon plan |
| Jaro semantics | PASS | SecondString oracle vectors, including null/empty |
| Exact phases | PASS | `findTrainingData`, `label`, `updateLabel`, `train`, `match`, `link`, `generateDocs` |
| Fuzzy feature/training/match/link/cluster path | PASS | Databricks run `71045321447523`: 3 candidate pairs; Exact + Jaro features; 1 positive/2 negative; direct/pre-scored match and link each return 1 pair; accepted edge yields 1 cluster containing 2 records; cross-source link returns 2 pairs |
| Photon Exact/Jaccard execution | PASS | `PhotonRange`/`PhotonProject`; Photon says fully supported |
| Photon fuzzy execution | FALLBACK | Run `734731292536358` confirms semantics, but Jaro's nested `aggregate` makes the fuzzy plan fall back |

## Open gates

| Gate | Result | Reason |
|---|---|---|
| Jaro Photon execution | FALLBACK | Photon rejects the required nested `aggregate` expression |
| Affine Gap native implementation | OPEN | Upstream Monge-Elkan/AffineGap uses dynamic programming and approximate character scoring; no faithful Photon-safe expression is implemented |
| Fabric Runtime 2 / Gluten+Velox | DEFERRED | Explicitly outside the current validation scope |
| Full upstream phase parity | LIMITED | Native fuzzy feature/threshold path is tested, but it is not a replacement for every Zingg 0.7 trainer/model implementation |

The supported Databricks claim is limited to the tested native Exact and
Jaccard workflow on Serverless environment 4 with Photon. Jaro is
available as semantically validated Spark SQL with an explicit Photon fallback.

Fabric validation source is prepared in `examples/fabric_runtime2_e2e.py` but
is not part of the current release gate.
# ARCHITECTURE REMEDIATION IN PROGRESS — NOT RELEASE READY

This repository is not yet a releasable Zingg Native implementation. The
current Python-only expression prototype is retained for comparison while the
shared Scala core, Classic/Py4J transport, Spark Connect transport, semantic
oracle, and reproducible integration gates are rebuilt.

No Databricks Serverless or Connect support claim is valid until a custom
server-side extension is actually installable and tested. Previous Serverless
wheel runs are prototype evidence only.
