# Implementation notes — 0.3.0-SNAPSHOT

This revision implements the transparent Databricks Photon + Serverless adapter around the real Zingg 0.7.0 Spark workflow.

## Implemented production path

- one public-expression semantic rewrite registry shared by Dedicated and Serverless;
- OFF/AUDIT/REWRITE/STRICT policy, with STRICT fail-closed;
- patched real Zingg Spark choke points rather than a second phase engine;
- exact upstream semantic class mapping instead of ambiguous UDF-name mapping;
- public-expression similarities, hash/blocking rules, stop words, vector extraction, blocking-tree compilation, and relational connected components;
- public-DataFrame model training/CV/prediction with Spark-compatible polynomial ordering and native sidecar persistence;
- Dedicated/local converter for existing Zingg `CrossValidatorModel` assets;
- native Serverless branches avoid DataFrame cache/persist/checkpoint;
- managed SparkSession lifecycle with no SparkContext ownership in native mode;
- privacy-safe rule diagnostics, normalized plan fingerprints, and strict forbidden-node guard;
- Dedicated/Py4J transport plumbing and Serverless/Spark Connect public-API transport;
- Serverless `DatabricksSession` launcher delegating to the real Zingg main;
- Maven profiles for Spark 4.0/4.1, Dedicated runtime lines, and Serverless;
- Asset Bundle templates for Dedicated Photon and Serverless;
- overlay application and dependency-injection script;
- capability manifest that distinguishes Serverless runtime evidence from
  unvalidated semantic parity and Dedicated capability claims.

## Deliberately unchanged

Zingg owns phase orchestration, candidate/label logic, blocking-tree semantics/artifact ownership, match/link differences, feature definitions, and ordinary user-facing APIs. Native mode substitutes the Spark-specific model implementation because that execution boundary is not viable for the mandatory Serverless Scala path; the replacement remains internal to `SparkModel`.

## Validation status

Databricks-only validation has been executed with CLI profile `sda`: Maven
artifacts were built, the bundle validated, static/bytecode checks passed, and
real Zingg 0.7.0 Serverless environment-5 jobs completed for
`findTrainingData`, bounded synthetic `train`, `match`, and `link`. Job-linked
query-history metrics provide Photon runtime evidence. Legacy prototype
tests/evidence are archived under `reference/legacy-*` and are not evidence
for this implementation. Semantic oracle/differential parity and Dedicated
Photon certification remain open release gates.
