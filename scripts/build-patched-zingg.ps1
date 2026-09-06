param(
  [string]$Output = 'dist/zingg-0.7.0-spark4-native.jar'
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
$lockPath = Join-Path $repo 'reference\upstream-lock.json'
$lock = Get-Content $lockPath -Raw | ConvertFrom-Json
$reference = Join-Path $repo $lock.path
if (-not (Test-Path (Join-Path $reference '.git'))) { throw 'Pinned reference checkout is missing. Run ./scripts/prepare-reference.ps1 first.' }
$actual = (git -C $reference rev-parse HEAD).Trim()
if ($actual -ne $lock.commit) { throw "Pinned reference mismatch: $actual" }
$actualTree = (git -C $reference rev-parse HEAD^{tree}).Trim()
if ($actualTree -ne $lock.tree) { throw "Pinned reference tree mismatch: $actualTree" }
$nativeCoreOverlay = Join-Path $repo 'integration\zingg-0.7.0-overlay'
if (-not (Test-Path $nativeCoreOverlay)) { throw 'Native overlay is missing.' }
$lockScalaVersion = [string]$lock.scalaVersion
$lockJavaRelease = [string]$lock.javaRelease
if ([string]::IsNullOrWhiteSpace($lockScalaVersion)) { throw 'Pinned reference lock is missing scalaVersion.' }
if ([string]::IsNullOrWhiteSpace($lockJavaRelease)) { throw 'Pinned reference lock is missing javaRelease.' }
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("zingg-native-build-" + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
try {
  git -C $reference archive --format=tar HEAD | tar -xf - -C $tempRoot
  if ($LASTEXITCODE -ne 0) { throw 'Unable to materialize the pinned upstream tree.' }
  Copy-Item -Path (Join-Path $nativeCoreOverlay '*') -Destination $tempRoot -Recurse -Force
  $pomPath = Join-Path $tempRoot 'pom.xml'
  $pom = Get-Content $pomPath -Raw
  $pom = $pom -replace '<maven.compiler.source>.*?</maven.compiler.source>', "<maven.compiler.source>$lockJavaRelease</maven.compiler.source>"
  $pom = $pom -replace '<maven.compiler.target>.*?</maven.compiler.target>', "<maven.compiler.target>$lockJavaRelease</maven.compiler.target>"
  $pom = $pom -replace '<scala.version>.*?</scala.version>', "<scala.version>$lockScalaVersion</scala.version>"
  $pom = $pom -replace '<spark.version>.*?</spark.version>', '<spark.version>4.0.1</spark.version>'
  Set-Content -LiteralPath $pomPath -Value $pom -NoNewline
  $sparkPomPath = Join-Path $tempRoot 'spark\pom.xml'
  $sparkPom = Get-Content $sparkPomPath -Raw
  $sparkPom = $sparkPom -replace '<spark.version>.*?</spark.version>', '<spark.version>4.0.1</spark.version>'
  Set-Content -LiteralPath $sparkPomPath -Value $sparkPom -NoNewline
  $commonPomPath = Join-Path $tempRoot 'common\pom.xml'
  $commonPom = Get-Content $commonPomPath -Raw
  $commonPom = $commonPom -replace '<spark.version>.*?</spark.version>', '<spark.version>4.0.1</spark.version>'
  Set-Content -LiteralPath $commonPomPath -Value $commonPom -NoNewline
  $assemblyPomPath = Join-Path $tempRoot 'assembly\pom.xml'
  if (Test-Path $assemblyPomPath) {
    $assemblyPom = Get-Content $assemblyPomPath -Raw
    $assemblyPom = $assemblyPom -replace '<spark.version>.*?</spark.version>', '<spark.version>4.0.1</spark.version>'
    Set-Content -LiteralPath $assemblyPomPath -Value $assemblyPom -NoNewline
  }
  $mvn = if ($IsWindows) { 'mvn.cmd' } else { 'mvn' }
  $sparkFrame = Join-Path $tempRoot 'spark\client\src\main\java\zingg\spark\client\SparkFrame.java'
  $frameText = Get-Content $sparkFrame -Raw
  # Spark 4 Serverless cannot execute the legacy Dataset cache path. Remove the
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
    # Skip test execution but keep test compilation/packaging enabled because
    # upstream modules consume sibling `tests` classifier artifacts.
    & $mvn '-DskipTests' "-Dscala.version=$lockScalaVersion" "-Djava.version=$lockJavaRelease" 'clean' 'package'
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
  Move-Item -Force $assembly $Output
} finally {
  if (Test-Path $tempRoot) { Remove-Item -Recurse -Force $tempRoot }
}
