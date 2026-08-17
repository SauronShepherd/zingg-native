# Databricks Asset Bundle

The repository includes a parameterized Serverless-only Asset Bundle. It
keeps the workspace artifact directory and Unity Catalog output volume out of
the job definition itself:

```powershell
./scripts/publish-databricks-serverless.ps1
databricks bundle validate -t sda
databricks bundle deploy -t sda
databricks bundle run -t sda zingg_native_serverless_core_e2e
```

The publish step is required before deployment: the bundle references the
paired workspace JARs, and publishing them first prevents a successful job
from accidentally executing stale artifacts.

Override `artifact_root` and `output_volume` for another workspace or volume.
The bundle creates a new job; the existing numeric job and its authoritative
run evidence remain documented separately in
`docs/evidence/databricks-serverless.json`.

The bundle is deliberately a JAR task. It does not imply that Databricks
managed Spark Connect can load the `ExpressionPlugin`.

The latest real deployment and run passed on 2026-08-17:

- Bundle job `190949869955356`, latest run `807529510025681`;
- task run `889856143647722`;
- Spark 4.1.0 with all three similarities, declarative preprocessing, the
  four shared phases, and
  five-positive/five-negative training-evidence readiness and Unity Catalog
  Volume persistence.
