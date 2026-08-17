$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$maven = Join-Path $repo '.tools\apache-maven-3.9.11\bin\mvn.cmd'
if (-not (Test-Path $maven)) {
  if ($env:MAVEN_HOME) { $maven = Join-Path $env:MAVEN_HOME 'bin\mvn.cmd' } else { $maven = 'mvn' }
}
$profile = if ($env:DATABRICKS_PROFILE) { $env:DATABRICKS_PROFILE } else { 'sda' }
$artifactRoot = if ($env:ZINGG_NATIVE_DATABRICKS_ARTIFACT_ROOT) {
  $env:ZINGG_NATIVE_DATABRICKS_ARTIFACT_ROOT
} else {
  '/Workspace/Users/angel.alvarez@sdggroup.com/zingg-native/remediation'
}

Push-Location $repo
try {
  & $maven '-q' '-DskipTests' 'package'
  if ($LASTEXITCODE -ne 0) { throw "Maven package failed: $LASTEXITCODE" }

  $core = 'core/target/zingg-native-core_2.13-0.2.0-SNAPSHOT.jar'
  $connect = 'connect/target/zingg-native-connect_2.13-0.2.0-SNAPSHOT.jar'
  foreach ($jar in @($core, $connect)) {
    if (-not (Test-Path $jar)) { throw "Expected artifact missing: $jar" }
    $name = Split-Path $jar -Leaf
    databricks workspace import "$artifactRoot/$name" --file $jar --format RAW --overwrite -p $profile
    if ($LASTEXITCODE -ne 0) { throw "Databricks upload failed for ${name}: $LASTEXITCODE" }
  }
  Write-Output "Published current JVM artifacts to $artifactRoot using profile $profile."
} finally { Pop-Location }
