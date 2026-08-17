# Dedicated Photon E2E runbook

The Asset Bundle defines `zingg_native_dedicated_photon_e2e` as the Dedicated
Photon execution gate. It uses Spark 15.4 with Scala 2.13 and Photon, and
stores output under the configured Unity Catalog Volume.

Run it with:

```powershell
databricks bundle validate -t sda
databricks bundle deploy -t sda
databricks bundle run -t sda zingg_native_dedicated_photon_e2e
```

After a successful run, populate `docs/evidence/photon-evidence.template.json`
from the Databricks run and query-profile/plan evidence. A successful JAR
completion alone is not Photon evidence. The evidence must identify the
workspace, job, run, task, runtime, profile, native rewrites, and zero
fallbacks for claimed operations.
