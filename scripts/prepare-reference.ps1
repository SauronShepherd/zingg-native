$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$lockPath = Join-Path $repo 'reference\zingg-0.7.0.lock'
$checkout = Join-Path $repo 'reference\upstream-zingg'
$lock = @{}
Get-Content $lockPath | Where-Object { $_ -match '^(\w+)=(.*)$' } | ForEach-Object { $lock[$Matches[1]] = $Matches[2] }
if (-not (Test-Path (Join-Path $checkout '.git'))) {
  # The upstream repository contains generated fixtures with Windows-hostile
  # paths. Keep the semantic source only; never check out the fixture tree.
  git clone --no-checkout $lock.repository $checkout
  git -C $checkout config core.longpaths true
  git -C $checkout sparse-checkout init --no-cone
  git -C $checkout sparse-checkout set '/common/core/src/main/**' '/common/client/src/main/**' '/spark/core/src/main/**' '/thirdParty/**'
}
git -C $checkout fetch --tags --quiet
git -C $checkout checkout --quiet --detach $lock.commit
$actual = (git -C $checkout rev-parse HEAD).Trim()
if (-not $actual.StartsWith($lock.commit)) { throw "Reference SHA mismatch: expected prefix $($lock.commit), got $actual" }
$dirty = git -C $checkout status --porcelain
if ($dirty) { throw "Reference checkout is dirty; refusing to use it as an oracle.`n$dirty" }
Write-Output "Zingg $($lock.version) reference ready at $actual"
