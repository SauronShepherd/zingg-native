$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$reference = Join-Path $repo 'reference\upstream-zingg'
$inventory = Join-Path $repo 'native-operation-inventory.yaml'
if (-not (Test-Path (Join-Path $reference '.git'))) { throw 'Pinned upstream checkout missing.' }
if (-not (Test-Path $inventory)) { throw 'native-operation-inventory.yaml missing.' }
$required = @(
  'spark/core/src/main/java/zingg/spark/core/similarity/SparkBaseTransformer.java',
  'spark/core/src/main/java/zingg/spark/core/hash/SparkHashFunction.java',
  'spark/core/src/main/java/zingg/spark/core/util/SparkHashUtil.java',
  'spark/core/src/main/java/zingg/spark/core/util/SparkBlockingTreeUtil.java',
  'spark/core/src/main/java/zingg/spark/core/block/SparkBlockFunction.java',
  'spark/core/src/main/java/zingg/spark/core/preprocess/stopwords/SparkStopWordsRemover.java',
  'spark/core/src/main/java/zingg/spark/core/model/VectorValueExtractor.java',
  'spark/core/src/main/java/zingg/spark/core/context/ZinggSparkContext.java'
)
foreach ($path in $required) {
  if (-not (Test-Path (Join-Path $reference $path))) { throw "Inventory source missing: $path" }
  $listed = Select-String -Path $inventory -Pattern $path -SimpleMatch
  if (-not $listed) { throw "Upstream non-native source is not listed: $path" }
}
$dirty = git -C $reference status --porcelain
if ($dirty) { throw "Reference checkout is dirty; inventory refused.`n$dirty" }
Write-Output "Upstream non-native operation inventory passed for $($required.Count) choke points."
