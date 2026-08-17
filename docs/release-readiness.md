# Release readiness

**ARCHITECTURE REMEDIATION IN PROGRESS — NOT RELEASE READY**

## Completed implementation gates

- Shared Scala 2.13/Spark 4.1 Maven build exists.
- Classic/Py4J gateway exists and has real local execution evidence.
- Exact, Jaccard, and Jaro are implemented in the shared core.
- Jaro matches pinned Zingg 0.7 / SecondString oracle vectors locally.
- Connect Exact payload serialization and plugin compilation are covered.
- Shared TRIM and CASE_NORMALIZE preprocessing is verified through Classic,
  self-managed Connect, and Databricks Serverless.
- Classic shared-core candidate, label, and update-label phases pass a real
  local Py4J E2E example; this is adapted phase evidence, not upstream parity.
- Labeled-record pair expansion (`buildTrainingPairs`) and genuine 5/5
  positive/negative evidence pass the Serverless E2E; blocking-tree learning
  and model fitting remain open.
- Unverified Python phase shortcuts are prototype-only.
- Versioned model and blocking-tree artifact contracts are defined and exposed
  through the capability handshake; no trainer parity is claimed.
- Training-evidence inspection and the five/five upstream minimum are executed
  in the Serverless E2E; model fitting is still not implemented.
- Python and Scala unit suites pass locally.
- Python Ruff/Mypy gates and artifact SHA-256 generation are wired into CI and
  pass locally.
- The CI clean-wheel job now executes the real Classic/Py4J candidate-phase E2E
  after installing the wheel, with the separately built shared-core JAR.

## Open mandatory gates

- Real Zingg 0.7 workflow integration through the native rewrite hook.
- Dedicated Photon E2E with supported runtime evidence.
- Serverless real Zingg E2E using only supported public Spark APIs.
- Self-managed Spark Connect Jaro and phase E2E with the installed plugin.
- Connect parity for Jaro and the shared phase subset.
- Full upstream Zingg 0.7 phase/model parity.

Databricks Serverless shared-core JAR execution is verified only for the
narrow historical slice; it is not full Zingg parity or Photon evidence.
Dedicated/Photon is a mandatory target and remains unverified.
