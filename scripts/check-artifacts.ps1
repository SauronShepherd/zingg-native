$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$core = Join-Path $repo 'core\target\zingg-native-core_2.13-0.3.0-SNAPSHOT.jar'
$launcher = Join-Path $repo 'serverless-launcher\target\zingg-native-serverless-launcher_2.13-0.3.0-SNAPSHOT.jar'
foreach ($artifact in @($core, $launcher)) {
  if (-not (Test-Path -LiteralPath $artifact)) { throw "Missing production JVM artifact: $artifact" }
}
python (Join-Path $repo 'scripts\verify-artifacts.py') --core $core --serverless-launcher $launcher
if ($LASTEXITCODE -ne 0) { throw "Artifact verification failed: $LASTEXITCODE" }
