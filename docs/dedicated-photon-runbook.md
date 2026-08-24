# Dedicated Photon runbook skeleton

Use `resources/dedicated-photon-zingg-native.yml` through the Asset Bundle after building/uploading the native-core and patched-Zingg JARs and supplying the real Zingg main class/arguments.

The current `sda` workspace rejects Dedicated/job-cluster compute with `Only serverless compute is supported in the workspace.` This is recorded as an environment blocker in [docs/evidence/databricks-serverless-v5.json](evidence/databricks-serverless-v5.json); it is not Dedicated Photon certification.
