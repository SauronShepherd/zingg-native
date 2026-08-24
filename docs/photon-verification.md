# Photon verification contract

The implementation contains planning/evidence hooks but does not claim that a successful job proves Photon execution.

`NativePlanGuard` can reject known callback/object-encoder nodes such as Scala/Python UDFs, Python evaluation nodes, `MapElements`, `MapPartitions`, object serialization/deserialization, and `ExistingRDD` in STRICT mode. `NativeEvidenceCollector` records applied rewrite IDs and a normalized plan fingerprint without logging row values.

The current v73 evidence combines adapter records with job-linked Databricks
query-history metrics containing nonzero `photon_total_time_ms` for the
successful Serverless train, label, updateLabel, match, and link tasks. Some
queries report zero Photon time, and the available API result does not map
operator attribution to individual rewrite rules. This proves Photon
participation, but not zero fallback or operator-level certification; the
rewrite-level gate therefore remains `unverified`.
