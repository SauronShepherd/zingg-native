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
