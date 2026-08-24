# Remediation status

**Scope:** Databricks Dedicated + Photon and Databricks Serverless.

**Architecture:** real Zingg remains the engine; `zingg-native` replaces only non-native Spark execution constructs at known Zingg choke points.

**Implementation status:** implemented in source.

**Validation status:** Databricks Serverless environment 5 has immutable release-set
20260820-1205/1155 immutable release evidence covering 15 similarity families over 18 edge-case
rows, 29 typed hash-oracle rules, and nontrivial graph/iteration cases, with stable plan
and output fingerprints. The runtime compatibility evidence confirms managed Spark Connect,
Spark 4.1.0, Scala 2.13, and Java 17. Full phase-level parity, complete model
training/persistence, remaining rule families, and authoritative operator-level
Photon evidence remain unverified. Dedicated Photon is not run in this
workspace per the Serverless-only test scope. A clean bounded fixture also
completed ordinary-Zingg train, label, updateLabel, match, and link phases on
Serverless through an explicit headless input seam.

The corrected full fixture now has terminal Serverless evidence on release
20260823-assembly-boundary1: production validation and an independent soak;
20260823-blocking-family2: current Serverless regression release with full blocking-family oracle coverage
completed the 20-feature/1,770-term initial fit plus full CV grid, native model
persistence save, and SUCCESS. A separate current-release model probe passed
fit/save/load/predict, and a second Serverless run loaded the saved Volume model
and predicted successfully. Historical canceled runs remain recorded as
historical observations; they do not override the later successful release
evidence in `docs/evidence/databricks-serverless-v5.json`.
