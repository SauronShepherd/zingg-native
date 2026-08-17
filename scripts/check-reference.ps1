$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$checkout = Join-Path $repo 'reference\upstream-zingg'
if (-not (Test-Path (Join-Path $checkout '.git'))) { throw 'Reference checkout missing; run scripts/prepare-reference.ps1 first.' }
$versionFile = Join-Path $repo 'reference\upstream-zingg.version'
if (-not (Test-Path $versionFile)) { throw 'Upstream version manifest missing.' }
$version = @{}
Get-Content $versionFile | Where-Object { $_ -match '^([^=]+)=(.*)$' } | ForEach-Object { $version[$Matches[1]] = $Matches[2] }
if ($version['version'] -ne '0.7.0' -or $version['checkout'] -ne 'reference/upstream-zingg') { throw 'Unexpected upstream version manifest.' }
$dirty = git -C $checkout status --porcelain
if ($dirty) { throw "Reference checkout is dirty; oracle execution refused.`n$dirty" }
$expected = ((Get-Content (Join-Path $repo 'reference\zingg-0.7.0.lock') | Where-Object { $_ -like 'commit=*' }) -split '=', 2)[1]
$actual = (git -C $checkout rev-parse HEAD).Trim()
if (-not $actual.StartsWith($expected)) { throw "Reference SHA mismatch: expected prefix $expected, got $actual" }
Write-Output "Reference clean and pinned: $actual"
