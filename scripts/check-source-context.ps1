$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$reference = Join-Path $repo 'reference\upstream-zingg'
$overlay = Join-Path $repo 'integration\zingg-0.7.0-overlay'
$lockPath = Join-Path $repo 'reference\zingg-0.7.0-spark4.lock'

if (-not (Test-Path (Join-Path $reference '.git'))) { throw 'Pinned reference checkout is missing.' }
if (-not (Test-Path $overlay)) { throw 'Integration overlay is missing.' }
if (-not (Test-Path $lockPath)) { throw 'Reference lock is missing.' }

$lock = @{}
Get-Content -LiteralPath $lockPath | Where-Object { $_ -match '^([^=]+)=(.*)$' } | ForEach-Object {
  $lock[$Matches[1]] = $Matches[2]
}
$actualCommit = (git -C $reference rev-parse HEAD).Trim()
if ($lock['commit'] -ne $actualCommit) { throw "Reference commit drift: lock=$($lock['commit']) actual=$actualCommit" }

function Get-TreeDigest([string]$root) {
  $entries = @()
  Get-ChildItem -LiteralPath $root -Recurse -File | Sort-Object FullName | ForEach-Object {
    $relative = $_.FullName.Substring($root.Length).TrimStart('\','/') -replace '\\','/'
    $hash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    $entries += "$relative`0$hash"
  }
  $bytes = [Text.Encoding]::UTF8.GetBytes(($entries -join "`n"))
  $sha = [Security.Cryptography.SHA256]::Create()
  try { return ([BitConverter]::ToString($sha.ComputeHash($bytes)) -replace '-','').ToLowerInvariant() }
  finally { $sha.Dispose() }
}

$referenceDigest = Get-TreeDigest $reference
$overlayDigest = Get-TreeDigest $overlay
if ($lock['referenceTreeDigest'] -ne $referenceDigest) {
  throw "Reference tree drift: lock=$($lock['referenceTreeDigest']) actual=$referenceDigest"
}
if ($lock['overlayTreeDigest'] -ne $overlayDigest) {
  throw "Overlay tree drift: lock=$($lock['overlayTreeDigest']) actual=$overlayDigest"
}

$checked = 0
Get-ChildItem -LiteralPath $overlay -Recurse -File | ForEach-Object {
  $relative = $_.FullName.Substring($overlay.Length).TrimStart('\','/')
  $base = Join-Path $reference $relative
  if (-not (Test-Path -LiteralPath $base)) { throw "Overlay source has no pinned upstream context: $relative" }
  $checked++
}
if ($checked -eq 0) { throw 'Overlay contains no source files.' }
Write-Output "Source context passed: commit=$actualCommit overlayFiles=$checked referenceTree=$referenceDigest overlayTree=$overlayDigest"
