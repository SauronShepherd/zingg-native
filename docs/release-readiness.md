# Release readiness

**ARCHITECTURE REMEDIATION IN PROGRESS — NOT RELEASE READY**

## Completed implementation gates

- Shared Scala 2.13/Spark 4.1 Maven build exists.
- Classic/Py4J gateway exists and has real local execution evidence.
- Exact, Jaccard, and Jaro are implemented in the shared core.
- Jaro matches pinned Zingg 0.7 / SecondString oracle vectors locally.
- Connect Exact payload serialization and plugin compilation are covered.
- Classic shared-core candidate, label, and update-label phases pass a real
  local Py4J E2E example; this is adapted phase evidence, not upstream parity.
- Unverified Python phase shortcuts are prototype-only.
- Python and Scala unit suites pass locally.

## Open mandatory gates

- Self-managed Spark Connect Jaro and phase E2E with the installed plugin.
- Connect parity for Jaro and the shared phase subset.
- Databricks Dedicated + Photon compute with the core JAR installed.
- Photon execution evidence through Classic/Py4J, not a wheel-only Serverless run.
- Full upstream Zingg 0.7 phase/model parity.
- CI execution and artifact-install integration tests.

The `sda` workspace currently has no associated worker environments, so a
Dedicated Photon cluster cannot be created there. Databricks Serverless is not
claimed until managed server-plugin installation is proven.
