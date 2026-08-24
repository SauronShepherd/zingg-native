param(
  [Parameter(Mandatory=$true)][string]$ReleaseId,
  [string]$Profile = 'sda',
  [string]$Catalog = 'sda_dev',
  [string]$Schema = 'default',
  [string]$Volume = 'zingg_native_e2e_volume',
  [string]$LicensePath = '/Workspace/Shared/zingg-native/e2e/LICENSE',
  [switch]$Deploy
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Push-Location $repo
try {
  & pwsh -NoProfile -File scripts/build.ps1 -Target serverless
  if ($LASTEXITCODE -ne 0) { throw "Serverless native build failed: $LASTEXITCODE" }
  & pwsh -NoProfile -File scripts/build-patched-zingg.ps1 -Output dist/zingg-0.7.0-spark4-native.jar
  if ($LASTEXITCODE -ne 0) { throw "Patched Zingg build failed: $LASTEXITCODE" }
  python scripts/check-serverless-bytecode.py core/target/zingg-native-core_2.13-0.3.0-SNAPSHOT.jar serverless-launcher/target/zingg-native-serverless-launcher_2.13-0.3.0-SNAPSHOT.jar dist/zingg-0.7.0-spark4-native.jar
  if ($LASTEXITCODE -ne 0) { throw "Serverless bytecode scan failed: $LASTEXITCODE" }
  & pwsh -NoProfile -File scripts/publish-serverless-fixtures.ps1 -Profile $Profile -Catalog $Catalog -Schema $Schema -Volume $Volume -LicensePath $LicensePath
  if ($LASTEXITCODE -ne 0) { throw "Fixture publication failed: $LASTEXITCODE" }
  & pwsh -NoProfile -File scripts/publish-databricks-serverless.ps1 -PatchedZinggJar dist/zingg-0.7.0-spark4-native.jar -ReleaseId $ReleaseId -Profile $Profile
  if ($LASTEXITCODE -ne 0) { throw "Artifact publication failed: $LASTEXITCODE" }
  $bundleArgs = @('bundle','validate','-t','serverless','-p',$Profile,'--var',"release_id=$ReleaseId",'--var',"catalog=$Catalog",'--var',"schema=$Schema",'--var',"volume=$Volume",'--var',"license_path=$LicensePath")
  & databricks @bundleArgs
  if ($LASTEXITCODE -ne 0) { throw "Bundle validation failed: $LASTEXITCODE" }
  if ($Deploy) {
    $deployArgs = @('bundle','deploy','-t','serverless','-p',$Profile,'--var',"release_id=$ReleaseId",'--var',"catalog=$Catalog",'--var',"schema=$Schema",'--var',"volume=$Volume",'--var',"license_path=$LicensePath")
    & databricks @deployArgs
    if ($LASTEXITCODE -ne 0) { throw "Bundle deployment failed: $LASTEXITCODE" }
  }
  Write-Output "Serverless release pipeline completed: $ReleaseId"
} finally { Pop-Location }
