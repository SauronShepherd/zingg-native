[CmdletBinding()]
param(
  [string]$Profile = 'sda',
  [Parameter(Mandatory = $true)] [string[]]$TaskRunId,
  [string]$OutputPath,
  [switch]$RequirePhotonForAll
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
  $OutputPath = Join-Path $repo 'docs\evidence\serverless-photon-runtime.json'
}
$raw = databricks query-history list -p $Profile --include-metrics --max-results 1000 -o json
$history = $raw | ConvertFrom-Json
$queries = @($history.res)

function Get-TaskId($query) {
  $query.query_source.job_info.job_task_run_id
}
function Get-PhotonMs($query) {
  $value = $query.metrics.photon_total_time_ms
  if ($null -eq $value) { return 0 }
  return [int64]$value
}

$tasks = [ordered]@{}
foreach ($id in $TaskRunId) {
  $matched = @($queries | Where-Object { "$(Get-TaskId $_)" -eq $id })
  if ($matched.Count -eq 0) { throw "No query-history records found for Serverless task run $id." }
  $photon = @($matched | Where-Object { (Get-PhotonMs $_) -gt 0 })
  $zero = @($matched | Where-Object { (Get-PhotonMs $_) -eq 0 })
  if ($RequirePhotonForAll -and $zero.Count -gt 0) {
    throw "Task run $id has $($zero.Count) query-history records with zero photon_total_time_ms."
  }
  $tasks[$id] = [ordered]@{
    queryCount = $matched.Count
    queriesWithPhotonTime = $photon.Count
    zeroPhotonQueries = $zero.Count
    photonTotalTimeMs = [int64](($matched | ForEach-Object { Get-PhotonMs $_ } | Measure-Object -Sum).Sum)
    queryIds = @($matched | ForEach-Object { $_.query_id })
  }
}

$evidence = [ordered]@{
  schemaVersion = '1'
  target = 'databricks-serverless-photon'
  profile = $Profile
  collectedAtUtc = [DateTime]::UtcNow.ToString('o')
  source = 'Databricks query-history API with include_metrics=true'
  environmentKey = 'serverless_env5'
  tasks = $tasks
  operatorLevelPhoton = 'unverified'
  interpretation = 'Photon timing proves runtime participation for matched queries only; query-history metrics do not attribute operators to individual rewrite rules.'
}
$parent = Split-Path -Parent $OutputPath
if (-not (Test-Path $parent)) { New-Item -ItemType Directory -Path $parent | Out-Null }
$evidence | ConvertTo-Json -Depth 10 | Set-Content -Path $OutputPath -Encoding utf8
Write-Output "Wrote Serverless Photon evidence: $OutputPath"
