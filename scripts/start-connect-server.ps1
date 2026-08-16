$ErrorActionPreference = 'Stop'
if (-not $env:SPARK_HOME) { throw 'Set SPARK_HOME to a Spark 4.1 distribution.' }
$repo = Split-Path -Parent $PSScriptRoot
$core = Join-Path $repo 'core\target\zingg-native-core_2.13-0.2.0-SNAPSHOT.jar'
$connect = Join-Path $repo 'connect\target\zingg-native-connect_2.13-0.2.0-SNAPSHOT.jar'
foreach ($path in @($core, $connect)) { if (-not (Test-Path $path)) { throw "Missing artifact: $path. Run scripts/build.ps1 first." } }
$jars = "$core;$connect"
$class = 'ai.zingg.native.connect.ZinggNativeExpressionPlugin'
& (Join-Path $env:SPARK_HOME 'sbin\start-connect-server.cmd') '--packages' 'org.apache.spark:spark-connect_2.13:4.1.0' '--conf' "spark.jars=$jars" '--conf' "spark.connect.extensions.expression.classes=$class"
