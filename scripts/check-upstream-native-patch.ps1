$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$patch = Join-Path $repo 'reference\patches\0001-native-spark-transformer-provider.patch'
$source = Join-Path $repo 'reference\upstream-zingg\spark\core\src\main\java\zingg\spark\core\similarity\SparkBaseTransformer.java'
if (-not (Test-Path $patch)) { throw 'Upstream integration patch is missing.' }
if (-not (Test-Path $source)) { throw 'Pinned upstream transformer source is missing.' }
$text = Get-Content $patch -Raw
foreach ($required in @('ai.zingg.nativebridge.NativeOperationProvider', 'NativeOperationProvider.fromSpark', 'similarityByZinggName', '"OFF".equals')) {
  if ($text -notlike "*$required*") { throw "Integration patch is missing required seam: $required" }
}
if ((Get-Content $patch | Select-String '^diff --git ').Count -ne 1) { throw 'Integration patch must target exactly one upstream file.' }
Write-Output 'Upstream native transformer integration patch passed structural checks.'
