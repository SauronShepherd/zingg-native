$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$path = Join-Path $repo 'docs\evidence\photon-evidence.template.json'
$e = Get-Content $path -Raw | ConvertFrom-Json
foreach ($name in @('schemaVersion','target','result','environment','run','plan','rewrites','fallbacks')) {
  if ($null -eq $e.$name) { throw "Photon evidence is missing: $name" }
}
if ($e.schemaVersion -ne '1') { throw 'Unsupported Photon evidence schema.' }
if ($e.result -eq 'PASS') {
  foreach ($name in @('workspaceId','jobId','runId','taskRunId')) {
    if ([string]::IsNullOrWhiteSpace($e.run.$name)) { throw "PASS evidence requires run.$name" }
  }
  if ([string]::IsNullOrWhiteSpace($e.plan.evidenceSource)) { throw 'PASS evidence requires plan.evidenceSource' }
  if ($e.fallbacks.Count -gt 0) { throw 'PASS evidence cannot contain native-operation fallbacks.' }
}
Write-Output "Photon evidence template passed: $($e.target) / $($e.result)"

$runtimePath = Join-Path $repo 'docs\evidence\serverless-photon-runtime.json'
if (Test-Path $runtimePath) {
  $runtime = Get-Content $runtimePath -Raw | ConvertFrom-Json
  foreach ($name in @('schemaVersion','target','profile','source','environmentKey','tasks','operatorLevelPhoton')) {
    if ($null -eq $runtime.$name) { throw "Serverless Photon runtime evidence is missing: $name" }
  }
  if ($runtime.target -ne 'databricks-serverless-photon') { throw 'Runtime Photon evidence target is not Serverless.' }
  if ($runtime.operatorLevelPhoton -ne 'unverified') { throw 'Runtime evidence must not overclaim operator-level Photon certification.' }
  if (@($runtime.tasks.PSObject.Properties).Count -eq 0) { throw 'Runtime Photon evidence contains no task records.' }
  foreach ($task in $runtime.tasks.PSObject.Properties) {
    if ($task.Value.queryCount -lt 1) { throw "Task $($task.Name) has no query records." }
    if ($task.Value.photonTotalTimeMs -lt 1) { throw "Task $($task.Name) has no Photon timing evidence." }
  }
  Write-Output "Serverless Photon runtime evidence passed: $($runtime.profile) / tasks=$(@($runtime.tasks.PSObject.Properties).Count)"
}
