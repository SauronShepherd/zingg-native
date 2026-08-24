# Serverless runbook skeleton

Use `resources/serverless-zingg-native.yml` after building/uploading the Serverless launcher, native-core, and patched-Zingg JARs. Supply the real Zingg delegate main class and its normal arguments.

The task invokes `DatabricksZinggMain`, which creates the managed session and delegates to real Zingg. It does not use the legacy custom Connect plugin.

Recorded Serverless evidence is maintained in [docs/evidence/databricks-serverless-v5.json](evidence/databricks-serverless-v5.json). Environment 5 evidence covers `findTrainingData`, bounded synthetic `train`, `match`, and `link` in STRICT mode. Photon evidence is based on job-linked query-history metrics and does not certify every operator. For graph phases, the launcher derives a temporary workspace below ordinary `--zinggDir`; pass `--native-graph-materialize-path` only when a different Volume location is required.

## Cross-job model persistence probe

For an isolated public-API model boundary check, run the launcher with
`--native-model-probe --native-model-probe-path <Volume path>`; this fits and
saves the degree-3 model. In a separate Serverless task, use
`--native-model-load-probe --native-model-probe-path <same Volume path>`; the
load-only task must emit `NATIVE_MODEL_PROBE_LOAD_PASS`. The explicit path is
for persistence validation only; ordinary probes remain run-isolated by
default.
