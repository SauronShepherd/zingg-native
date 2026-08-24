$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$path = Join-Path $repo 'reference\zingg-0.7-phase-contract.json'
$contract = Get-Content $path -Raw | ConvertFrom-Json
if ($contract.version -ne '0.7.0') { throw 'Phase contract is not pinned to Zingg 0.7.0.' }
$expected = @('findTrainingData', 'label', 'train', 'match', 'link', 'updateLabel')
$actual = @($contract.phases | ForEach-Object { $_.id })
if ((Compare-Object $expected $actual).Count -ne 0) { throw 'Zingg phase contract is incomplete or reordered.' }
if ($contract.activation.systemProperty -ne 'zingg.native.mode' -or
    $contract.activation.serverlessLauncherFlag -ne '--native-mode' -or
    $contract.activation.localOrDedicatedEnvironmentFallback -ne 'ZINGG_NATIVE_MODE' -or
    $contract.activation.default -ne 'STRICT') { throw 'Native activation contract is incomplete.' }
$required = @('similarity.udf','similarity.registration','hash.udf','hash.registration','blocking-tree.map','stopwords.udf','vector-extractor.udf','graphframes.connected-components','spark-context','model.pipeline','model.persistence','serverless.cache')
if ((Compare-Object $required @($contract.nativeChokePoints)).Count -ne 0) { throw 'Native choke-point inventory is incomplete.' }
Write-Output 'Zingg phase ownership and native choke-point contract passed.'
