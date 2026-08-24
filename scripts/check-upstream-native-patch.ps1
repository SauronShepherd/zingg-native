$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$patch = Join-Path $repo 'reference\patches\0002-zingg-0.7.0-photon-native-integration.patch'
if (-not (Test-Path $patch)) { throw 'Focused upstream integration patch is missing.' }
$text = Get-Content $patch -Raw
$required = @(
  'SparkBaseTransformer.java', 'SparkSimFunction.java', 'SparkTransformer.java',
  'SparkHashFunction.java', 'SparkHashUtil.java', 'SparkStopWordsRemover.java',
  'SparkBlockingTreeUtil.java', 'VectorValueExtractor.java', 'SparkModel.java', 'SparkGraphUtil.java',
  'ZinggSparkContext.java', 'ai.zingg.nativebridge.NativeOperationProvider'
)
foreach ($needle in $required) {
  if ($text -notlike "*$needle*") { throw "Integration patch is missing required content: $needle" }
}
Write-Output 'Focused Zingg 0.7 native integration patch has all expected choke points.'
