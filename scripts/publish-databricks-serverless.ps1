param(
  [Parameter(Mandatory=$true)][string]$PatchedZinggJar,
  [string]$ArtifactRoot = '/Workspace/Shared/zingg-native',
  [string]$ReleaseId = 'dev-current',
  [string]$Profile = $env:DATABRICKS_PROFILE
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($Profile)) { throw 'Supply -Profile or DATABRICKS_PROFILE.' }
$core = Join-Path $repo 'core\target\zingg-native-core_2.13-0.3.0-SNAPSHOT.jar'
$launcher = Join-Path $repo 'serverless-launcher\target\zingg-native-serverless-launcher_2.13-0.3.0-SNAPSHOT.jar'
$releaseRoot = "$ArtifactRoot/releases/$ReleaseId"
$manifestPath = Join-Path ([System.IO.Path]::GetTempPath()) ("zingg-native-manifest-" + [guid]::NewGuid().ToString() + '.json')
$entries = @()
$uploaded = @()
databricks workspace mkdirs $releaseRoot -p $Profile
if ($LASTEXITCODE -ne 0) { throw "Unable to create Serverless release directory $releaseRoot" }
foreach ($jar in @($core, $launcher, $PatchedZinggJar)) {
  if (-not (Test-Path -LiteralPath $jar)) { throw "Missing artifact: $jar" }
  $name = Split-Path $jar -Leaf
  $sha = (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant()
  databricks workspace import "$releaseRoot/$name" --file $jar --format RAW --overwrite -p $Profile
  if ($LASTEXITCODE -ne 0) { throw "Databricks upload failed for ${name}: $LASTEXITCODE" }
  $uploaded += [ordered]@{ name=$name; localSha=$sha; remotePath="$releaseRoot/$name" }
  $entries += [ordered]@{ name=$name; sha256=$sha; bytes=(Get-Item -LiteralPath $jar).Length }
}
foreach ($item in $uploaded) {
  $download = Join-Path ([System.IO.Path]::GetTempPath()) ("zingg-remote-" + [guid]::NewGuid().ToString('N') + '.bin')
  try {
    databricks workspace export $item.remotePath --format AUTO --file $download -p $Profile
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $download)) { throw "Unable to export uploaded artifact $($item.name) for checksum verification." }
    $remoteSha = (Get-FileHash -LiteralPath $download -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($remoteSha -ne $item.localSha) { throw "Remote checksum mismatch for $($item.name): local=$($item.localSha) remote=$remoteSha" }
  } finally {
    if (Test-Path -LiteralPath $download) { Remove-Item -LiteralPath $download -Force }
  }
}
$nativeSha = (git -C $repo rev-parse HEAD).Trim()
$manifest = [ordered]@{
  releaseId=$ReleaseId
  nativeVersion='0.3.0-SNAPSHOT'
  nativeGitSha=$nativeSha
  zinggBaseline='reference/upstream-zingg'
  overlayPath='integration/zingg-0.7.0-overlay'
  scala='2.13.16'
  java='17'
  databricksConnect='18.0.0'
  environmentVersion='5'
  capabilitySchemaVersion='1'
  artifacts=$entries
} | ConvertTo-Json -Depth 6
Set-Content -LiteralPath $manifestPath -Value $manifest -Encoding utf8
databricks workspace import "$releaseRoot/manifest.json" --file $manifestPath --format RAW --overwrite -p $Profile
if ($LASTEXITCODE -ne 0) { throw "Databricks upload failed for manifest" }
Remove-Item -LiteralPath $manifestPath -Force
Write-Output "Published atomic Serverless release set $releaseRoot using profile $Profile."
