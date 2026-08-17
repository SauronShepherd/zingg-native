# Remediation evidence matrix

This matrix is the current release audit for the remediation plan. `Verified`
means there is executable evidence in this repository; `Open` means the
implementation or target-runtime proof is still missing.

| Gate | Status | Evidence | Notes |
|---|---|---|---|
| Shared Scala 2.13/Spark 4.1 core | Verified | `pom.xml`, `core/`, `mvn test` | Spark dependencies are provided by the target runtime |
| Classic/Py4J transport | Verified | `core/.../ClassicGateway.scala`, `python/.../classic.py`, `examples/classic_candidate_e2e.py` | Real local DataFrame execution |
| Connect protocol/plugin | Verified | `connect/src/main/protobuf/`, `ZinggNativeExpressionPlugin.scala`, `examples/connect_similarity_e2e.py` | Exact/Jaccard local Spark 4.1 E2E |
| Exact/Jaccard/Jaro shared similarities | Verified | `core/src/main/scala/.../Core.scala`, Scala/Python tests | Jaro Connect remains open |
| Classic declarative phases | Verified subset | `Core.findTrainingData`, `Core.label`, `Core.updateLabel` | Local Classic E2E; upstream parity not certified |
| Python prototype isolation | Verified | `backend/base.py`, `_prototype_phase` guards, boundary tests | Prototype requires explicit `backend="expressions"` |
| Artifact reproducibility | Verified | `scripts/check-artifacts.ps1`, CI workflow | Wheel excludes Spark runtime |
| Upstream semantic reference | Verified | `reference/zingg-0.7.0.lock`, `reference/upstream-zingg/` | Pinned 0.7.0 source checkout |
| Databricks artifact presence | Verified | `sda` workspace imports and SHA-256 checks | Presence is not execution evidence |
| Databricks Dedicated + Photon execution | Out of scope | Project target is Serverless only | No Photon claim |
| Databricks Serverless/managed Connect | Open pending feasibility gate | No proven server-plugin installation path | No Serverless claim |
| Full Zingg phase/model parity | Open | `docs/phase-map.md` | Training, matching, linking, clustering remain unimplemented in shared core |
