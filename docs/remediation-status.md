# Remediation evidence matrix

This matrix is the current release audit for the remediation plan. `Verified`
means there is executable evidence in this repository; `Open` means the
implementation or target-runtime proof is still missing.

| Gate | Status | Evidence | Notes |
|---|---|---|---|
| Shared Scala 2.13/Spark 4.1 core | Verified | `pom.xml`, `core/`, `mvn test` | Spark dependencies are provided by the target runtime |
| Classic/Py4J transport | Verified | `core/.../ClassicGateway.scala`, `python/.../classic.py`, `examples/classic_candidate_e2e.py` | Real local DataFrame execution |
| Connect protocol/plugin | Verified | `connect/src/main/protobuf/`, `ZinggNativeExpressionPlugin.scala`, `examples/connect_similarity_e2e.py` | Exact/Jaccard/TRIM/CASE_NORMALIZE local Spark 4.1 E2E |
| Exact/Jaccard/Jaro shared similarities | Verified | `core/src/main/scala/.../Core.scala`, Scala/Python tests | Jaro Connect remains open |
| Shared declarative preprocessing | Verified Classic + Connect + Serverless | `Core.preprocess`, both transports, Serverless Bundle run `168088194011842` | TRIM and CASE_NORMALIZE execute as standard Spark expressions |
| Classic declarative phases | Verified subset | `Core.findTrainingData`, `Core.label`, `Core.updateLabel` | Local Classic E2E; upstream parity not certified |
| Versioned model/blocking artifact contracts | Verified contract | `core/.../Artifacts.scala`, capability manifest, `ArtifactContractTest` | Schema and five-positive/five-negative validation exist; fitting/persistence remain open |
| Training evidence readiness | Verified prerequisite | `Core.buildTrainingPairs`, `Core.inspectTrainingEvidence`, Serverless Bundle run `807529510025681` | Five-positive/five-negative pairs derived by labeled-record self-join on Spark 4.1.0; model fitting remains open |
| Experimental model fit seam | Implemented, not certified | `ExperimentalModelTrainer.scala` | Spark ML logistic regression over supplied features; upstream preprocessing/blocking parity remains open |
| Python prototype isolation | Verified | `backend/base.py`, `_prototype_phase` guards, boundary tests | Prototype requires explicit `backend="expressions"` |
| Artifact reproducibility | Verified | `scripts/verify-artifacts.py`, `scripts/artifact-checksums.py`, CI workflow | Wheel excludes Spark runtime; CI publishes SHA-256 manifest |
| Python lint/type gates | Verified locally and wired to CI | Ruff, Mypy, `.github/workflows/ci.yml` | Ruff and Mypy pass on the current source tree |
| Upstream semantic reference | Verified | `reference/zingg-0.7.0.lock`, `reference/upstream-zingg/` | Pinned 0.7.0 source checkout |
| Databricks artifact presence | Verified | `sda` workspace imports and SHA-256 checks | Presence is not execution evidence |
| Databricks Dedicated + Photon execution | Out of scope | Project target is Serverless only | No Photon claim |
| Databricks Serverless/managed Connect | Unsupported by current managed path | Job `177009162619307`, task `1075870166806203` rejected plugin config with `CONFIG_NOT_AVAILABLE` | No managed-plugin claim; requires a Databricks-supported extension mechanism |
| Databricks Serverless shared-core JAR task | Verified narrow slice | Job `295665184144562`, run `847651604040137` | Exact/Jaccard/Jaro plus findTrainingData/label/updateLabel and UC-volume persistence executed on Spark 4.1.0; Connect plugin remains open |
| Databricks Serverless Asset Bundle deployment | Verified expanded slice | Bundle job `190949869955356`, run `807529510025681` | Automated publish, deploy, and run sequence completed with shared preprocessing and labeled-pair expansion on Serverless; managed Connect remains open |
| Full Zingg phase/model parity | Open | `docs/phase-map.md` | Training, matching, linking, clustering remain unimplemented in shared core |
