param(
  [string]$FixtureRoot = '/Workspace/Shared/zingg-native/e2e',
  [string]$Profile = $env:DATABRICKS_PROFILE,
  [string]$LicensePath = '/Workspace/Shared/zingg-native/e2e/LICENSE',
  [string]$Catalog = 'sda_dev',
  [string]$Schema = 'default',
  [string]$Volume = 'zingg_native_e2e_volume'
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($Profile)) { throw 'Supply -Profile or DATABRICKS_PROFILE.' }
databricks workspace mkdirs $FixtureRoot -p $Profile
$volumeRoot = "/Volumes/$Catalog/$Schema/$Volume"
$sourceVolumeRoot = '/Volumes/sda_dev/default/zingg_native_e2e_volume'
$sourceFixtureRoot = '/Workspace/Shared/zingg-native/e2e'
$renderRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('zingg-native-fixtures-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $renderRoot | Out-Null
$mapping = [ordered]@{
  'config-four-feature-marked.json'='databricks-config-four-feature-marked.json'
  'config-one-fuzzy.json'='databricks-config-one-fuzzy.json'
  'config-full-parquet.json'='databricks-config-full-parquet.json'
  'config-minimal-parquet.json'='databricks-config-minimal-parquet.json'
  'config-minimal.json'='databricks-config-minimal-parquet.json'
  'config-volume.json'='databricks-config-full-parquet.json'
  'config-tiny-marked.json'='databricks-config-tiny-marked.json'
  'prepare-minimal-parquet.py'='databricks-prepare-minimal-parquet.py'
  'seed-minimal-parquet-labels.py'='databricks-seed-minimal-parquet-labels.py'
  'seed-minimal-labels.py'='databricks-seed-minimal-labels.py'
  'databricks-copy-four-feature-fixture.py'='databricks-copy-four-feature-fixture.py'
  'databricks-copy-full-feature-fixture.py'='databricks-copy-full-feature-fixture.py'
  'prepare-full-feature-fixture.py'='databricks-prepare-full-feature-fixture.py'
  'databricks-model-sparkml-oracle.py'='databricks-model-sparkml-oracle.py'
}
foreach ($target in $mapping.Keys) {
  $source = Join-Path $repo ('scripts\' + $mapping[$target])
  if (-not (Test-Path -LiteralPath $source)) { throw "Missing fixture source: $source" }
  $upload = $source
  $content = Get-Content -LiteralPath $source -Raw
  if ($content.Contains($sourceVolumeRoot) -or $content.Contains($sourceFixtureRoot)) {
    $rendered = $content.Replace($sourceVolumeRoot, $volumeRoot).Replace($sourceFixtureRoot, $FixtureRoot)
    $upload = Join-Path $renderRoot $target
    Set-Content -LiteralPath $upload -Value $rendered -Encoding utf8 -NoNewline
  }
  databricks workspace import "$FixtureRoot/$target" --file $upload --format RAW --overwrite -p $Profile
  if ($LASTEXITCODE -ne 0) { throw "Fixture upload failed: $target" }
}
databricks workspace get-status $LicensePath -p $Profile | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Required license material is missing: $LicensePath" }
if (Test-Path -LiteralPath $renderRoot) { Remove-Item -LiteralPath $renderRoot -Recurse -Force }
Write-Output "Published deterministic non-secret Serverless fixtures to $FixtureRoot."
