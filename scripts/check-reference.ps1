$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$checkout = Join-Path $repo 'reference\upstream-zingg'
if (-not (Test-Path (Join-Path $checkout '.git'))) { throw 'Reference checkout missing; run scripts/prepare-reference.ps1 first.' }
$dirty = git -C $checkout status --porcelain
if ($dirty) { throw "Reference checkout is dirty; oracle execution refused.`n$dirty" }
$expected = ((Get-Content (Join-Path $repo 'reference\zingg-0.7.0.lock') | Where-Object { $_ -like 'commit=*' }) -split '=', 2)[1]
$actual = (git -C $checkout rev-parse HEAD).Trim()
if (-not $actual.StartsWith($expected)) { throw "Reference SHA mismatch: expected prefix $expected, got $actual" }
Write-Output "Reference clean and pinned: $actual"
