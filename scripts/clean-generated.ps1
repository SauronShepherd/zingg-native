$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not (Test-Path (Join-Path $repo 'pom.xml'))) { throw "Refusing cleanup outside repository root: $repo" }
foreach ($relative in @('dist', 'build', 'core/target', 'serverless-launcher/target')) {
  $path = Join-Path $repo $relative
  if (Test-Path -LiteralPath $path) {
    Remove-Item -LiteralPath $path -Recurse -Force
    Write-Output "Removed generated path: $path"
  }
}
Get-ChildItem -Path $repo -Recurse -Directory -Filter '__pycache__' | Remove-Item -Recurse -Force
