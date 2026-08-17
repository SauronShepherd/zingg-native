$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$bundledMaven = Join-Path $repo '.tools\apache-maven-3.9.11\bin\mvn.cmd'
$maven = if ($env:MAVEN_HOME) {
  Join-Path $env:MAVEN_HOME 'bin\mvn.cmd'
} elseif (Test-Path $bundledMaven) {
  $bundledMaven
} else {
  'mvn'
}
Push-Location $repo
try {
  & $maven '-DskipTests' 'package'
  if ($LASTEXITCODE -ne 0) { throw "Maven build failed: $LASTEXITCODE" }
  python -m build --wheel
  if ($LASTEXITCODE -ne 0) { throw "Python wheel build failed: $LASTEXITCODE" }
} finally { Pop-Location }
