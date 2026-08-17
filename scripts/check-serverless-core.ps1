$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$jar = Join-Path $repo 'core\target\zingg-native-core_2.13-0.2.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar)) { throw "Missing common core JAR: $jar" }
$forbidden = @(
  'org/apache/spark/sql/catalyst/',
  'org/apache/spark/rdd/',
  'org/apache/spark/SparkContext',
  'org/apache/spark/api/java/JavaSparkContext',
  'org/apache/spark/sql/internal/',
  'org/apache/spark/sql/util/'
)
$strings = (& jar tf $jar) -join "`n"
$bytes = [System.IO.File]::ReadAllBytes((Resolve-Path $jar))
$text = [System.Text.Encoding]::Latin1.GetString($bytes)
foreach ($needle in $forbidden) {
  if ($text.Contains($needle)) { throw "Serverless common JAR contains forbidden reference: $needle" }
}
Write-Output "Serverless common JAR passed forbidden-reference check: $($jar)"
