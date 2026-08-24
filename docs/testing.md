# Validation status

Validation for this workspace is restricted to Databricks Serverless using CLI
profile `sda`. Dedicated and Fabric are not run in this workspace.

Verified evidence includes Serverless environment 5 startup (Spark 4.1.0,
Scala 2.13, Java 17, managed Spark Connect), a real Zingg 0.7.0 STRICT
differential probe covering seven similarity rules, source/bytecode boundaries,
bundle deployment, and strict fail-closed behavior for disabled rewrites.

The current release evidence verifies real Serverless `train`, `match`,
`link`, `label`, and `updateLabel` orchestration, including native
fit/save/load/prediction, cross-job model loading, graph rewrites, the
20-feature/1,770-term production training path, and an independent soak run.
These are runtime and release-line validations, not a blanket semantic
certification. Complete phase-level oracle parity, exhaustive model-oracle
parity, broad large-data readiness, Dedicated Photon execution, and
operator-level Photon attribution remain uncertified. Historical canceled or
diagnostic runs are retained as context and do not override the successful
current-release evidence.

## Reproducible Serverless runner

After deploying the bundle, run:

```powershell
pwsh -File scripts/run-serverless-e2e.ps1 -Profile sda -JobName zingg-native-serverless-differential
```

The runner discovers the deployed job, submits it asynchronously, polls the
Databricks run state, and returns the result. Evidence belongs in
`docs/evidence/databricks-serverless-v5.json`.
