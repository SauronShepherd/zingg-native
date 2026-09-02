param(
  [string]$Reference = (Join-Path (Split-Path -Parent $PSScriptRoot) 'reference\upstream-zingg'),
  [string]$Output = (Join-Path (Split-Path -Parent $PSScriptRoot) 'dist\zingg-0.7.0-spark4-native.jar')
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$bundledMaven = Join-Path $repo '.tools\apache-maven-3.9.11\bin\mvn.cmd'
$mvn = if ($env:MAVEN_HOME) { Join-Path $env:MAVEN_HOME 'bin\mvn.cmd' } elseif (Test-Path $bundledMaven) { $bundledMaven } else { 'mvn' }
if (-not (Test-Path (Join-Path $Reference '.git'))) { throw "Pinned reference checkout is missing: $Reference" }
$lockPath = Join-Path $repo 'reference\zingg-0.7.0-spark4.lock'
if (-not (Test-Path -LiteralPath $lockPath)) { throw "Pinned reference lock is missing: $lockPath" }
$lockCommit = ((Get-Content -LiteralPath $lockPath | Where-Object { $_ -like 'commit=*' }) -replace '^commit=', '').Trim()
$lockSparkVersion = ((Get-Content -LiteralPath $lockPath | Where-Object { $_ -like 'sparkVersion=*' }) -replace '^sparkVersion=', '').Trim()
$lockScalaVersion = ((Get-Content -LiteralPath $lockPath | Where-Object { $_ -like 'scalaVersion=*' }) -replace '^scalaVersion=', '').Trim()
$lockJavaRelease = ((Get-Content -LiteralPath $lockPath | Where-Object { $_ -like 'javaRelease=*' }) -replace '^javaRelease=', '').Trim()
if ($lockSparkVersion -ne '4.1.0' -or $lockScalaVersion -ne '2.13.16' -or $lockJavaRelease -ne '17') {
  throw "Reference lock is not aligned with Databricks Serverless environment 5: Spark=$lockSparkVersion Scala=$lockScalaVersion Java=$lockJavaRelease."
}
$referenceCommit = (git -C $Reference rev-parse HEAD).Trim()
if ($lockCommit -ne $referenceCommit) { throw "Reference checkout HEAD $referenceCommit does not match lock commit $lockCommit." }
$overlay = Join-Path $repo 'integration\zingg-0.7.0-overlay'
if (-not (Test-Path $overlay)) { throw "Integration overlay is missing: $overlay" }
$contextCheck = Join-Path $repo 'scripts\check-source-context.ps1'
& pwsh -NoProfile -File $contextCheck
if ($LASTEXITCODE -ne 0) { throw 'Pinned reference/overlay source-context verification failed.' }

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('zingg-native-build-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
  # Do not pipe a large git archive directly into the Windows tar shim: on
  # some hosts that truncates the stream and produces checksum failures.
  # Materializing the archive first keeps extraction deterministic.
  $archivePath = Join-Path ([System.IO.Path]::GetTempPath()) ('zingg-native-reference-' + [guid]::NewGuid().ToString('N') + '.tar')
  try {
    git -C $Reference archive --format=tar --output=$archivePath HEAD
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $archivePath)) { throw 'Unable to materialize the pinned Zingg reference archive.' }
    tar -xf $archivePath -C $tempRoot
    if ($LASTEXITCODE -ne 0) { throw 'Unable to extract the pinned Zingg reference tree.' }
  } finally {
    if (Test-Path -LiteralPath $archivePath) { Remove-Item -LiteralPath $archivePath -Force }
  }
  Get-ChildItem -LiteralPath $overlay -Force | Copy-Item -Destination $tempRoot -Recurse -Force
  $pom = Get-Content (Join-Path $tempRoot 'pom.xml') -Raw
  # Serverless environment 5 runs Spark 4.1.0 with Scala 2.13.16. Build the
  # patched Zingg assembly against the versions recorded in the lock.
  $pom = $pom -replace '<spark.version>3\.5\.5</spark.version>', "<spark.version>$lockSparkVersion</spark.version>"
  $pom = $pom -replace '<scala.version>2\.12\.10</scala.version>', "<scala.version>$lockScalaVersion</scala.version>"
  $pom = $pom -replace '<scala.version>2\.12\.17</scala.version>', "<scala.version>$lockScalaVersion</scala.version>"
  $pom = $pom -replace '<scala.binary.version>2\.12</scala.binary.version>', '<scala.binary.version>2.13</scala.binary.version>'
  $pom = $pom -replace 'jackson-module-scala_2\.12', 'jackson-module-scala_2.13'
  $pom = $pom -replace '<maven.compiler.source>11</maven.compiler.source>', "<maven.compiler.source>$lockJavaRelease</maven.compiler.source>"
  $pom = $pom -replace '<maven.compiler.target>11</maven.compiler.target>', "<maven.compiler.target>$lockJavaRelease</maven.compiler.target>"
  Set-Content -LiteralPath (Join-Path $tempRoot 'pom.xml') -Value $pom -NoNewline
  $sparkPomPath = Join-Path $tempRoot 'spark\pom.xml'
  $sparkPom = Get-Content $sparkPomPath -Raw
  $sparkPom = $sparkPom -replace 'graphframes-spark3_2\.12', 'graphframes-spark3_2.13'
  $sparkPom = $sparkPom -replace '(?s)<dependency>\s*<groupId>io\.graphframes</groupId>.*?</dependency>', ''
  Set-Content -LiteralPath $sparkPomPath -Value $sparkPom -NoNewline
  $sparkFrame = Join-Path $tempRoot 'spark\client\src\main\java\zingg\spark\client\SparkFrame.java'
  $frame = Get-Content $sparkFrame -Raw
  if ($frame -notmatch 'zingg\.native\.managed') {
    $frame = $frame -replace '(public ZFrame<Dataset<Row>, Row, Column> cache\(\) \{)', ('$1' + [Environment]::NewLine + '        if (Boolean.parseBoolean(System.getProperty("zingg.native.managed", "false"))) return new SparkFrame(df);')
    Set-Content -LiteralPath $sparkFrame -Value $frame -NoNewline
  }
  if ((Get-Content $sparkFrame -Raw) -notmatch 'zingg\.native\.managed') { throw 'Serverless SparkFrame cache guard was not applied.' }
  $client = Join-Path $tempRoot 'common\client\src\main\java\zingg\common\client\Client.java'
  $clientText = Get-Content $client -Raw
  if ($clientText -notmatch 'managedInvocation') {
    $clientText = $clientText -replace '(public abstract class Client[^\{]*\{)', ('$1' + [Environment]::NewLine + '    private static boolean managedInvocation() { return Boolean.parseBoolean(System.getProperty("zingg.native.managed", "false")); }')
    $clientText = $clientText -replace '(LOG\.warn\(options\.getHelp\(\)\);)', ('$1' + [Environment]::NewLine + '                if (managedInvocation()) return;')
    $clientText = $clientText -replace '(if \(success\) \{\r?\n\s+System\.exit\(0\);\r?\n\s+\} else \{\r?\n\s+System\.exit\(1\);\r?\n\s+\})', ('if (managedInvocation() && success) return;' + [Environment]::NewLine + '            if (managedInvocation()) throw new IllegalStateException("Zingg processing failed in managed invocation");' + [Environment]::NewLine + '            $1')
    $clientText = $clientText -replace '(catch \(ZinggClientException e\) \{\r?\n\s+System\.exit\(1\);\r?\n\s+\})', ('catch (ZinggClientException e) {' + [Environment]::NewLine + '            if (!managedInvocation()) System.exit(1);' + [Environment]::NewLine + '        }')
    # The upstream finally block has additional exit sites outside
    # cleanupAndExit().  In a managed Serverless invocation those exits must
    # become returns so the Databricks task owns lifecycle and failure state.
    $clientLines = $clientText -split "`r?`n"
    $clientLines = $clientLines | ForEach-Object {
      if ($_ -match 'System\.exit\((0|1)\);' -and $_ -notmatch 'managedInvocation\(\)') {
        $_ -replace 'System\.exit\((0|1)\);', 'if (!managedInvocation()) System.exit($1);'
      } else { $_ }
    }
    $clientText = $clientLines -join [Environment]::NewLine
    Set-Content -LiteralPath $client -Value $clientText -NoNewline
  }
  if ((Get-Content $client -Raw) -notmatch 'managedInvocation') { throw 'Serverless Client lifecycle guard was not applied.' }
  if ((Get-Content $client -Raw) -split "`r?`n" | Where-Object { $_ -match 'System\.exit\((0|1)\);' -and $_ -notmatch 'managedInvocation\(\)' }) { throw 'An unguarded managed Client System.exit remains.' }
  $stopWords = Join-Path $tempRoot 'common\client\src\main\java\zingg\common\client\HasStopWords.java'
  (Get-Content $stopWords -Raw) -replace 'import scala\.Serializable;', 'import java.io.Serializable;' | Set-Content -LiteralPath $stopWords -NoNewline
  $arrayFeature = Join-Path $tempRoot 'common\core\src\main\java\zingg\common\core\feature\ArrayDoubleFeature.java'
  (Get-Content $arrayFeature -Raw) -replace 'scala\.collection\.mutable\.WrappedArray', 'scala.collection.Seq' -replace 'BaseFeature<WrappedArray<Double>>', 'BaseFeature<Seq<Double>>' | Set-Content -LiteralPath $arrayFeature -NoNewline
  $arraySimilarity = Join-Path $tempRoot 'common\core\src\main\java\zingg\common\core\similarity\function\ArrayDoubleSimilarityFunction.java'
  $arrayText = Get-Content $arraySimilarity -Raw
  $arrayText = $arrayText -replace 'scala\.collection\.mutable\.WrappedArray', 'scala.collection.Seq'
  $arrayText = $arrayText -replace 'WrappedArray<Double>', 'Seq<Double>'
  $arrayText = $arrayText -replace '(?s)\s+@Override\s+public Double call\(Seq<Double> t1, Seq<Double> t2\) \{.*?\n\s+\}\s+\n\}', @'

    @Override
    public Double call(Seq<Double> t1, Seq<Double> t2) {
        Double[] t1Arr = new Double[t1 == null ? 0 : t1.size()];
        Double[] t2Arr = new Double[t2 == null ? 0 : t2.size()];
        for (int i = 0; i < t1Arr.length; i++) t1Arr[i] = t1.apply(i);
        for (int i = 0; i < t2Arr.length; i++) t2Arr[i] = t2.apply(i);
        return call(t1Arr, t2Arr);
    }
}
'@
  Set-Content -LiteralPath $arraySimilarity -Value $arrayText -NoNewline
  $frameText = Get-Content $sparkFrame -Raw
  $frameText = $frameText -replace '(?s)    @Override\s+public ZFrame<Dataset<Row>, Row, Column> withColumns\(String\[\] columns, Column\[\] columnValues\) \{.*?\n    \}', @'
    @Override
    public ZFrame<Dataset<Row>, Row, Column> withColumns(String[] columns, Column[] columnValues) {
        Dataset<Row> result = df;
        for (int i = 0; i < columns.length; i++) result = result.withColumn(columns[i], columnValues[i]);
        return new SparkFrame(result);
    }
'@
  $frameText = $frameText -replace 'df\.repartition\(num, partitionExprs\)', 'df.repartition(num, scala.collection.JavaConverters.seqAsJavaListConverter(partitionExprs).asJava().toArray(new Column[0]))'
  $frameText = $frameText -replace 'df\.repartition\(partitionExprs\)', 'df.repartition(scala.collection.JavaConverters.seqAsJavaListConverter(partitionExprs).asJava().toArray(new Column[0]))'
  # The patched assembly is the Serverless production artifact.  Remove the
  # unsupported cache call entirely, rather than relying only on a runtime
  # property guard around legacy SparkFrame.cache().
  $frameText = $frameText -replace '(?s)public ZFrame<Dataset<Row>, Row, Column> cache\(\) \{.*?\n    \}', @'
public ZFrame<Dataset<Row>, Row, Column> cache() {
        return new SparkFrame(df);
    }
'@
  Set-Content -LiteralPath $sparkFrame -Value $frameText -NoNewline
  $blockingTree = Join-Path $tempRoot 'common\core\src\main\java\zingg\common\core\util\BlockingTreeUtil.java'
  $blockingTreeText = Get-Content $blockingTree -Raw
  if ($blockingTreeText -notmatch 'positives = positives\.coalesce\(1\);') { throw 'Expected upstream blocking-tree coalesce(1) boundary was not found.' }
  $blockingTreeText = $blockingTreeText -replace '\s*positives = positives\.coalesce\(1\);', ''
  Set-Content -LiteralPath $blockingTree -Value $blockingTreeText -NoNewline
  $block = Join-Path $tempRoot 'spark\core\src\main\java\zingg\spark\core\block\SparkBlockFunction.java'
  (Get-Content $block -Raw) -replace 'scala\.collection\.JavaConversions', 'scala.collection.JavaConverters' -replace 'JavaConversions\.seqAsJavaList\(sObj\)', 'JavaConverters.seqAsJavaListConverter(sObj).asJava()' | Set-Content -LiteralPath $block -NoNewline
  $nativeCore = Join-Path $repo 'core\target\zingg-native-core_2.13-0.3.0-SNAPSHOT.jar'
  if (-not (Test-Path $nativeCore)) { throw "Build native core first: $nativeCore" }
  & $mvn install:install-file "-Dfile=$nativeCore" '-DgroupId=ai.zingg' '-DartifactId=zingg-native-core_2.13' '-Dversion=0.3.0-SNAPSHOT' '-Dpackaging=jar' '-DgeneratePom=true' '-q'
  if ($LASTEXITCODE -ne 0) { throw 'Unable to install the native bridge API for the isolated assembly build.' }
  $bridgeDependency = '<dependency><groupId>ai.zingg</groupId><artifactId>zingg-native-core_2.13</artifactId><version>0.3.0-SNAPSHOT</version><scope>provided</scope></dependency>'
  $sparkPom = Get-Content $sparkPomPath -Raw
  $sparkPom = $sparkPom -replace '(<dependencies>)', ('$1' + [Environment]::NewLine + $bridgeDependency)
  Set-Content -LiteralPath $sparkPomPath -Value $sparkPom -NoNewline
  Push-Location $tempRoot
  try {
    # Spark modules declare test classifiers from both common-client and
    # common-core. Build and install only those fixture paths with tests
    # skipped, avoiding incompatible legacy Spark test sources while keeping
    # the production build deterministic.
    & $mvn '-DskipTests' "-Dscala.version=$lockScalaVersion" "-Djava.version=$lockJavaRelease" '-pl' 'common/client,common/core' '-am' 'install'
    if ($LASTEXITCODE -ne 0) { throw "Unable to install required upstream test fixtures: $LASTEXITCODE" }
    & $mvn '-Dmaven.test.skip=true' "-Dscala.version=$lockScalaVersion" "-Djava.version=$lockJavaRelease" 'clean' 'package'
    if ($LASTEXITCODE -ne 0) { throw "Patched Zingg Maven build failed: $LASTEXITCODE" }
  } finally { Pop-Location }
  $staging = Join-Path $tempRoot 'serverless-assembly-staging'
  New-Item -ItemType Directory -Force -Path $staging | Out-Null
  foreach ($classes in @('common\infra\target\classes','common\client\target\classes','common\core\target\classes','spark\client\target\classes','spark\core\target\classes','assembly\target\classes')) {
    $classesPath = Join-Path $tempRoot $classes
    if (Test-Path $classesPath) { Get-ChildItem -LiteralPath $classesPath -Force | Copy-Item -Destination $staging -Recurse -Force }
  }
  $secondString = Join-Path $tempRoot 'thirdParty\lib\secondstring.jar'
  if (Test-Path $secondString) {
    Push-Location $staging
    & jar xf $secondString
    Pop-Location
    if ($LASTEXITCODE -ne 0) { throw 'Unable to include the required secondstring runtime dependency.' }
  } else { throw "Pinned Zingg dependency is missing: $secondString" }
  $assembly = Join-Path $tempRoot 'serverless-assembly.jar'
  # JDK 17's fixed entry date makes the release JAR reproducible across runs.
  & jar --create --file $assembly --date=2000-01-01T00:00:00Z -C $staging .
  if ($LASTEXITCODE -ne 0 -or -not (Test-Path $assembly)) { throw 'Patched Zingg Serverless assembly JAR was not produced.' }
  New-Item -ItemType Directory -Force -Path (Split-Path $Output) | Out-Null
  Copy-Item -LiteralPath $assembly -Destination $Output -Force
  python (Join-Path $repo 'scripts\check-zingg-assembly.py') $Output
  python (Join-Path $repo 'scripts\check-serverless-bytecode.py') $nativeCore (Join-Path $repo 'serverless-launcher\target\zingg-native-serverless-launcher_2.13-0.3.0-SNAPSHOT.jar') $Output
  Get-FileHash -Algorithm SHA256 $Output
} finally {
  if (Test-Path $tempRoot) { Remove-Item -LiteralPath $tempRoot -Recurse -Force }
}