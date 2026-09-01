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
  Get-ChildItem -LiteralPath $root -Recurse -File |
    Where-Object { $_.FullName -notmatch '[\\/]\.git([\\/]|$)' } |
    Sort-Object FullName | ForEach-Object {
    $relative = $_.FullName.Substring($root.Length).TrimStart('\','/') -replace '\\','/'
    # Hash logical text content so the lock is stable across Windows CRLF and
    # Linux LF checkouts. These trees contain source/configuration files only.
    $content = [Text.Encoding]::UTF8.GetString([IO.File]::ReadAllBytes($_.FullName)) -replace "`r`n?", "`n"
    $contentBytes = [Text.Encoding]::UTF8.GetBytes($content)
    $hashAlgorithm = [Security.Cryptography.SHA256]::Create()
    try { $hash = ([BitConverter]::ToString($hashAlgorithm.ComputeHash($contentBytes)) -replace '-','').ToLowerInvariant() }
    finally { $hashAlgorithm.Dispose() }
    $entries += "$relative`0$hash"
  }
  $bytes = [Text.Encoding]::UTF8.GetBytes(($entries -join "`n"))
  $sha = [Security.Cryptography.SHA256]::Create()
  try { return ([BitConverter]::ToString($sha.ComputeHash($bytes)) -replace '-','').ToLowerInvariant() }
  finally { $sha.Dispose() }
}

function Get-GitTreeDigest([string]$root) {
  $lines = @(git -C $root ls-tree -r --full-tree HEAD)
  if ($LASTEXITCODE -ne 0 -or $lines.Count -eq 0) { throw "Unable to read pinned reference tree: $root" }
  $bytes = [Text.Encoding]::UTF8.GetBytes(($lines -join "`n"))
  $sha = [Security.Cryptography.SHA256]::Create()
  try { return ([BitConverter]::ToString($sha.ComputeHash($bytes)) -replace '-','').ToLowerInvariant() }
  finally { $sha.Dispose() }
}

$referenceDigest = Get-GitTreeDigest $reference
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
