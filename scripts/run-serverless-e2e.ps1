param(
  [string]$Profile = 'sda',
  [string]$JobName = 'zingg-native-serverless-differential',
  [int]$TimeoutMinutes = 20
)
$ErrorActionPreference = 'Stop'

function Invoke-DbxJson([string[]]$Arguments) {
  $raw = & databricks @Arguments -p $Profile -o json
  if ($LASTEXITCODE -ne 0) { throw "Databricks CLI failed: databricks $($Arguments -join ' ')" }
  return ($raw | ConvertFrom-Json)
}

$jobs = @(Invoke-DbxJson @('jobs','list'))
$job = $jobs | Where-Object { $_.settings.name -like "*$JobName*" } | Select-Object -First 1
if ($null -eq $job) { throw "No deployed Serverless job matched '$JobName'." }
$before = @(Invoke-DbxJson @('jobs','list-runs','--job-id', [string]$job.job_id, '--limit', '1'))
$beforeId = if ($before.Count -gt 0) { [string]$before[0].run_id } else { '' }

# run-now can remain attached to Serverless startup; the run list is the
# authoritative asynchronous handoff and avoids treating an empty CLI reply
# as a failed submission.
$null = Start-Job -ScriptBlock {
  param($profile,$jobId)
  & databricks jobs run-now $jobId -p $profile | Out-Null
} -ArgumentList $Profile,$job.job_id

$deadline = (Get-Date).AddMinutes($TimeoutMinutes)
$run = $null
while ((Get-Date) -lt $deadline) {
  Start-Sleep -Seconds 5
  $runs = @(Invoke-DbxJson @('jobs','list-runs','--job-id', [string]$job.job_id, '--limit', '5'))
  $run = $runs | Where-Object { [string]$_.run_id -ne $beforeId } | Select-Object -First 1
  if ($null -ne $run) { break }
}
if ($null -eq $run) { throw "Timed out waiting for Serverless job '$JobName' to submit." }

while ((Get-Date) -lt $deadline) {
  $current = Invoke-DbxJson @('jobs','get-run', [string]$run.run_id)
  if ($current.state.life_cycle_state -in @('TERMINATED','SKIPPED','INTERNAL_ERROR')) {
    $current | ConvertTo-Json -Depth 20
    if ($current.state.result_state -ne 'SUCCESS') { exit 1 }
    exit 0
  }
  Start-Sleep -Seconds 10
}
throw "Timed out waiting for Serverless job run $($run.run_id)."
