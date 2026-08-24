$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$jar = Join-Path $repo 'core\target\zingg-native-core_2.13-0.3.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar)) { throw "Missing Serverless native-core JAR: $jar" }
python (Join-Path $repo 'scripts\check-serverless-bytecode.py') $jar
if ($LASTEXITCODE -ne 0) { throw "Serverless bytecode check failed: $LASTEXITCODE" }
