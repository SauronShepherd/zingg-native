param(
  [ValidateSet('core','spark40','spark41','dedicated17','dedicated18','serverless','converter')]
  [string]$Target = 'core',
  [switch]$Wheel
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$bundledMaven = Join-Path $repo '.tools\apache-maven-3.9.11\bin\mvn.cmd'
$maven = if ($env:MAVEN_HOME) { Join-Path $env:MAVEN_HOME 'bin\mvn.cmd' } elseif (Test-Path $bundledMaven) { $bundledMaven } else { 'mvn' }
$profile = switch ($Target) {
  'spark40' { 'spark40' }
  'spark41' { 'spark41' }
  'dedicated17' { 'databricks-dedicated-17.3' }
  'dedicated18' { 'databricks-dedicated-18-lts' }
  'serverless' { 'databricks-serverless-env5' }
  'converter' { 'legacy-model-converter' }
  default { $null }
}
Push-Location $repo
try {
  # Production artifact builds must not compile the local prototype-era test
  # tree. Runtime validation is performed by the Databricks jobs only.
  $args = @('-Dmaven.test.skip=true', 'package')
  if ($profile) { $args = @("-P$profile") + $args }
  & $maven @args
  if ($LASTEXITCODE -ne 0) { throw "Maven build failed: $LASTEXITCODE" }
  if ($Wheel) {
    python -m build --wheel
    if ($LASTEXITCODE -ne 0) { throw "Python wheel build failed: $LASTEXITCODE" }
  }
} finally { Pop-Location }
