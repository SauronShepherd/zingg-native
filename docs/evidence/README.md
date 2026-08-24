# Evidence for the current adapter

`databricks-serverless-v5.json` records the current real-Zingg Serverless
execution. It proves task-level execution and rewrite observability. Photon
runtime coverage is promoted only where Databricks query-history metrics for
the actual job-linked queries report nonzero `photon_total_time_ms`.

Files under `reference/legacy-evidence/` belong to the previous prototype and
must never be used to promote current capabilities.
