$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$mvn = Join-Path $repo '.tools\apache-maven-3.9.11\bin\mvn.cmd'
if (-not (Test-Path -LiteralPath $mvn)) { $mvn = 'mvn' }
$output = Join-Path ([System.IO.Path]::GetTempPath()) ('zingg-native-dependency-tree-' + [guid]::NewGuid().ToString('N') + '.txt')
try {
  & $mvn '-Pdatabricks-serverless-env5' '-pl' 'serverless-launcher' '-am' 'dependency:tree' '-DincludeScope=runtime' '-DoutputType=text' '-DskipTests' '-Dmaven.test.skip=true' *> $output
  if ($LASTEXITCODE -ne 0) { throw "Serverless dependency-tree resolution failed: $LASTEXITCODE" }
  $text = Get-Content -LiteralPath $output -Raw
  foreach ($forbidden in @('graphframes', '_2.12', 'spark-connect-server-plugin', 'zingg-native-connect')) {
    if ($text -match [regex]::Escape($forbidden)) { throw "Forbidden Serverless dependency resolved: $forbidden" }
  }
  if ($text -notmatch 'scala-library:jar:2\.13\.16:provided') { throw 'Scala 2.13.16 is not the resolved Serverless provided dependency.' }
  if ($text -notmatch 'databricks-connect_2\.13:jar:18\.0\.0:provided') { throw 'Databricks Connect 18.0.0 is not the resolved Serverless provided dependency.' }
  Write-Output 'Serverless dependency boundary: clean'
} finally {
  if (Test-Path -LiteralPath $output) { Remove-Item -LiteralPath $output -Force }
}
