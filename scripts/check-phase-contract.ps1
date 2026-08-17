$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$path = Join-Path $repo 'reference\zingg-0.7-phase-contract.json'
$contract = Get-Content $path -Raw | ConvertFrom-Json
if ($contract.version -ne '0.7.0') { throw 'Phase contract is not pinned to Zingg 0.7.0.' }
$expected = @('findTrainingData', 'label', 'train', 'match', 'link')
$actual = @($contract.phases | ForEach-Object { $_.id })
if ((Compare-Object $expected $actual).Count -ne 0) { throw 'Zingg phase contract is incomplete or reordered.' }
if ($contract.activation.environmentVariable -ne 'ZINGG_NATIVE_MODE') { throw 'Serverless-safe activation contract missing.' }
$required = @('similarity.exact', 'similarity.jaccard', 'similarity.jaro', 'preprocess.trim', 'preprocess.case_normalize', 'blocking.hash', 'blocking.tree', 'preprocess.stopwords', 'model.vector_extraction', 'link.connected_components')
if ((Compare-Object $required @($contract.requiredOperations)).Count -ne 0) { throw 'Phase contract operation inventory is incomplete.' }
Write-Output 'Zingg phase and activation contract passed.'
