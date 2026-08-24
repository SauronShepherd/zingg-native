$ErrorActionPreference = 'Stop'
if (-not $env:SPARK_HOME) { throw 'Set SPARK_HOME to a Spark 4.1 distribution.' }
$windowsHost = ($env:OS -eq 'Windows_NT') -or ($IsWindows -eq $true)
if ($windowsHost -and (-not $env:HADOOP_HOME -or -not (Test-Path (Join-Path $env:HADOOP_HOME 'bin\winutils.exe')))) {
  throw 'Windows Spark Connect startup requires HADOOP_HOME\bin\winutils.exe. Configure a Hadoop Windows utility directory before starting the server.'
}
$repo = Split-Path -Parent $PSScriptRoot
$core = Join-Path $repo 'core\target\zingg-native-core_2.13-0.2.0-SNAPSHOT.jar'
$connect = Join-Path $repo 'connect\target\zingg-native-connect_2.13-0.2.0-SNAPSHOT.jar'
foreach ($path in @($core, $connect)) { if (-not (Test-Path $path)) { throw "Missing artifact: $path. Run scripts/build.ps1 first." } }
$jars = "$core;$connect"
$class = 'ai.zingg.native.connect.ZinggNativeExpressionPlugin'
& (Join-Path $env:SPARK_HOME 'sbin\start-connect-server.cmd') '--packages' 'org.apache.spark:spark-connect_2.13:4.1.0' '--conf' "spark.jars=$jars" '--conf' "spark.connect.extensions.expression.classes=$class"
