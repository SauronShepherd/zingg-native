param(
  [long]$JobId = 558281169908458,
  [Parameter(Mandatory=$true)][string]$ReleaseId,
  [string]$Profile = 'sda'
)
$ErrorActionPreference = 'Stop'
$job = databricks jobs get $JobId -p $Profile --output json | ConvertFrom-Json
$root = "/Workspace/Shared/zingg-native/releases/$ReleaseId"
$job.settings.environments[0].spec.java_dependencies = @(
  "$root/zingg-native-serverless-launcher_2.13-0.3.0-SNAPSHOT.jar",
  "$root/zingg-native-core_2.13-0.3.0-SNAPSHOT.jar",
  "$root/zingg-0.7.0-spark4-native.jar"
)
$payload = [ordered]@{ job_id=$JobId; new_settings=$job.settings } | ConvertTo-Json -Depth 20 -Compress
databricks jobs reset --json $payload -p $Profile
if ($LASTEXITCODE -ne 0) { throw "Unable to reset job $JobId." }
Write-Output "Job $JobId now points to release $ReleaseId."
