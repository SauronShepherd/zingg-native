$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$lockPath = Join-Path $repo 'reference\zingg-0.7.0.lock'
$checkout = Join-Path $repo 'reference\upstream-zingg'
$lock = @{}
Get-Content $lockPath | Where-Object { $_ -match '^(\w+)=(.*)$' } | ForEach-Object { $lock[$Matches[1]] = $Matches[2] }
if (-not (Test-Path (Join-Path $checkout '.git'))) {
  git clone $lock.repository $checkout
}
git -C $checkout fetch --tags --quiet
git -C $checkout checkout --quiet --detach $lock.commit
$actual = (git -C $checkout rev-parse HEAD).Trim()
if ($actual -ne $lock.commit) { throw "Reference SHA mismatch: expected $($lock.commit), got $actual" }
$dirty = git -C $checkout status --porcelain
if ($dirty) { throw "Reference checkout is dirty; refusing to use it as an oracle.`n$dirty" }
Write-Output "Zingg $($lock.version) reference ready at $actual"
