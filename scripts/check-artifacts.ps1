$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot

$core = Join-Path $repo 'core\target\zingg-native-core_2.13-0.2.0-SNAPSHOT.jar'
$connect = Join-Path $repo 'connect\target\zingg-native-connect_2.13-0.2.0-SNAPSHOT.jar'
foreach ($artifact in @($core, $connect)) {
  if (-not (Test-Path -LiteralPath $artifact)) { throw "Missing JVM artifact: $artifact" }
}

$wheel = Get-ChildItem (Join-Path $repo 'dist') -Filter 'zingg_native-*.whl' -File |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $wheel) { throw 'No Python wheel found under dist; run scripts/build.ps1 first.' }

Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead($wheel.FullName)
try {
  $names = $zip.Entries | ForEach-Object FullName
  if ($names -match 'pyspark|spark-sql|spark-core') {
    throw 'Wheel contains Spark runtime files; Spark must remain provided by the target runtime.'
  }
  if (-not ($names -match 'zingg_native/backend/classic.py')) { throw 'Wheel is missing Classic transport.' }
  if (-not ($names -match 'zingg_native/backend/connect.py')) { throw 'Wheel is missing Connect transport.' }
} finally { $zip.Dispose() }

Write-Output "Artifacts valid: $($wheel.Name), $([IO.Path]::GetFileName($core)), $([IO.Path]::GetFileName($connect))"
