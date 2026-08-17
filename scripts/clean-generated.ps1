$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$expectedName = 'zingg-native'
if ((Split-Path $repo -Leaf) -ne $expectedName) {
  throw "Refusing cleanup outside the expected repository: $repo"
}

foreach ($relative in @('dist', 'build', 'core/target', 'connect/target')) {
  $path = Join-Path $repo $relative
  if (Test-Path -LiteralPath $path) {
    Remove-Item -LiteralPath $path -Recurse -Force
    Write-Output "Removed generated path: $path"
  }
}
