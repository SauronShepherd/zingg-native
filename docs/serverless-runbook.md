# Databricks Serverless E2E runbook

This runbook reproduces the verified shared-core E2E slice using the `sda`
Databricks CLI profile. It exercises Exact, Jaccard, and Jaro similarities,
`preprocess`, `findTrainingData`, `buildTrainingPairs`, `label`, `updateLabel`, and persistence through a Unity
Catalog Volume.

It does not claim managed Spark Connect plugin activation. That feasibility
gate is recorded separately in [databricks.md](databricks.md).

## Prerequisites

- Databricks CLI authenticated as profile `sda`.
- Serverless Environment 5 access.
- A writable Unity Catalog Volume. The recorded run uses
  `sda_dev.sandbox.zingg_native_e2e`.
- JDK 17+ and Maven for building the Scala artifacts.

## Build and upload

From the repository root, build both JARs and upload them as a pair to the
workspace artifact directory. The Connect artifact depends on the shared core
version.

```powershell
mvn -q package -DskipTests
databricks workspace import <workspace-core-jar> --profile sda --file <local-core-jar> --format AUTO --overwrite
databricks workspace import <workspace-connect-jar> --profile sda --file <local-connect-jar> --format AUTO --overwrite
```

## Configure and run

The checked-in job definition is `databricks-serverless-core-e2e.json`. For an
existing job, apply `databricks-serverless-core-e2e-reset.json`, then trigger
job `295665184144562` with the Databricks CLI.

The task writes to `/Volumes/sda_dev/sandbox/zingg_native_e2e/run`. Do not
replace this with `/tmp`: public DBFS root is disabled on the tested Serverless
environment and fails with `DBFS_DISABLED`.

## Verify

Retrieve the task output with `databricks jobs get-run-output <task-run-id>`.
The required output begins:

```text
ZINGG_NATIVE_SERVERLESS_CORE_E2E PASS similarities=exact,jaccard,jaro phases=findTrainingData,label,updateLabel persistence=true spark=4.1.0
```

The authoritative recorded evidence is in
`docs/evidence/databricks-serverless.json`.
