# Databricks Asset Bundle

The repository includes a parameterized Serverless-only Asset Bundle. It
keeps the workspace artifact directory and Unity Catalog output volume out of
the job definition itself:

```powershell
databricks bundle validate -t sda
databricks bundle deploy -t sda
databricks bundle run -t sda zingg_native_serverless_core_e2e
```

Override `artifact_root` and `output_volume` for another workspace or volume.
The bundle creates a new job; the existing numeric job and its authoritative
run evidence remain documented separately in
`docs/evidence/databricks-serverless.json`.

The bundle is deliberately a JAR task. It does not imply that Databricks
managed Spark Connect can load the `ExpressionPlugin`.
