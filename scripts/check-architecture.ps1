$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$pythonRoot = Join-Path $repo 'python\src\zingg_native'
$allowed = @(
  (Join-Path $pythonRoot 'backend\classic.py'),
  (Join-Path $pythonRoot 'backend\connect.py')
)
$violations = @()
Get-ChildItem -Path $pythonRoot -Recurse -Filter '*.py' | ForEach-Object {
  $path = $_.FullName
  $text = Get-Content -Raw $path
  if ($allowed -notcontains $path -and $text -match '(?m)(?<![A-Za-z0-9_])(?:_jvm|_jdf|_gateway)(?![A-Za-z0-9_])') {
    $violations += "${path}: private JVM handle found outside an approved transport"
  }
}
if ($violations.Count -gt 0) { throw ($violations -join [Environment]::NewLine) }
Write-Output 'Architecture boundary checks passed.'
