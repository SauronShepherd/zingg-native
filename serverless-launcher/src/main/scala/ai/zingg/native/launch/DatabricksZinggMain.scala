package ai.zingg.native.launch

import java.io.ByteArrayInputStream
import java.lang.reflect.{InvocationTargetException, Method}
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import com.databricks.connect.DatabricksSession
import org.apache.spark.sql.SparkSession

/**
 * Databricks Serverless JAR bootstrap for the real patched Zingg main class.
 *
 * Adapter flags are consumed locally and translated to JVM properties before
 * Zingg starts.  This is intentional: Serverless applications must not depend
 * on environment-variable propagation for native-mode activation.
 * Everything not prefixed as a launcher/native flag is forwarded unchanged to
 * the upstream Zingg entry point.
 *
 * Usage:
 *   DatabricksZinggMain \
 *     --delegate-main <zingg.main.Class> \
 *     --native-mode STRICT \
 *     [--native-run-id <id>] \
 *     [--native-disabled-rules a,b] \
 *     [--native-plan-guard true|false] \
 *     [zingg args...]
 */
object DatabricksZinggMain {
  private val DelegateFlag = "--delegate-main"
  private val NativeModeFlag = "--native-mode"
  private val NativeRunIdFlag = "--native-run-id"
  private val NativeDisabledRulesFlag = "--native-disabled-rules"
  private val NativePlanGuardFlag = "--native-plan-guard"
  private val NativeModelMaxIterFlag = "--native-model-max-iter"
  private val NativeModelQuickProbeFlag = "--native-model-quick-probe"
  private val NativeModelBoundedProbeFlag = "--native-model-bounded-probe"
  private val NativeModelParityProbeFlag = "--native-model-parity-probe"
  private val NativeGraphMaxIterationsFlag = "--native-graph-max-iterations"
  private val NativeGraphMaterializePathFlag = "--native-graph-materialize-path"
  private val NativeGraphStrategyFlag = "--native-graph-strategy"
  private val NativeDifferentialProbeFlag = "--native-differential-probe"
  private val NativeDifferentialRulesFlag = "--native-differential-rules"
  private val NativeModelProbeFlag = "--native-model-probe"
  private val NativeModelLoadProbeFlag = "--native-model-load-probe"
  private val NativeModelCorruptionProbeFlag = "--native-model-corruption-probe"
  private val NativeModelProbePathFlag = "--native-model-probe-path"
  private val NativeGraphProbeFlag = "--native-graph-probe"
  private val NativeGraphBenchmarkFlag = "--native-graph-benchmark"
  private val NativeLdbcStressFlag = "--native-ldbc-stress"
  private val NativeLdbcVerticesFlag = "--native-ldbc-vertices"
  private val NativeLdbcEdgesFlag = "--native-ldbc-edges"
  private val NativeLdbcExpectedFlag = "--native-ldbc-expected"
  private val NativeLdbcDatasetFlag = "--native-ldbc-dataset"
  private val NativeLdbcOutputFlag = "--native-ldbc-output"
  private val NativeHashDifferentialProbeFlag = "--native-hash-differential-probe"
  private val NativeNumericDifferentialProbeFlag = "--native-numeric-differential-probe"
  private val NativeDateArrayDifferentialProbeFlag = "--native-date-array-differential-probe"
  private val NativeStopWordsDifferentialProbeFlag = "--native-stopwords-differential-probe"
  private val NativeInputFormatProbeFlag = "--native-input-format-probe"
  private val NativeVectorProbeFlag = "--native-vector-probe"
  private val NativeBlockingDifferentialProbeFlag = "--native-blocking-differential-probe"
  private val NativeRowIdProbeFlag = "--native-row-id-probe"
  private val NativeFuzzyActionProbeFlag = "--native-fuzzy-action-probe"
  private val NativeMaterializationFailureProbeFlag = "--native-materialization-failure-probe"
  private val NativeMaterializationRecoveryProbeFlag = "--native-materialization-recovery-probe"
  private val NativeFuzzyActionsOnlyFlag = "--native-fuzzy-actions-only"
  private val NativeInteractiveInputFlag = "--native-interactive-input"

  private final case class LaunchArguments(
      delegateClass: String,
      forwarded: Array[String],
      nativeMode: String,
      runId: Option[String],
      disabledRules: Option[String],
      planGuard: Option[String],
      modelMaxIter: Option[String],
      modelQuickProbe: Option[String],
      graphMaxIterations: Option[String],
      graphMaterializePath: Option[String],
      differentialProbe: Boolean,
      modelProbe: Boolean,
      modelLoadProbe: Boolean,
      modelCorruptionProbe: Boolean,
      modelProbePath: Option[String],
      graphProbe: Boolean,
      graphBenchmark: Boolean,
      ldbcStress: Boolean,
      hashDifferentialProbe: Boolean,
      numericDifferentialProbe: Boolean,
      dateArrayDifferentialProbe: Boolean,
      stopWordsDifferentialProbe: Boolean,
      inputFormatProbe: Boolean,
      vectorProbe: Boolean,
      blockingDifferentialProbe: Boolean,
      rowIdProbe: Boolean,
      fuzzyActionProbe: Boolean,
      materializationFailureProbe: Boolean,
      materializationRecoveryProbe: Boolean,
      differentialRules: Option[String],
      interactiveInput: Option[String])

  def main(args: Array[String]): Unit = {
    val launch = resolveArguments(args)
    applyNativeSettings(launch)

    val spark: SparkSession = DatabricksSession.builder()
      .validateSession(false)
      .addCompiledArtifacts(DatabricksZinggMain.getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
      .getOrCreate()

    // Make the managed runtime contract part of the structured task evidence;
    // a successful connection alone is not sufficient proof that the expected
    // Serverless environment was used.
    val connectVersion = Option(DatabricksSession.getClass.getPackage)
      .flatMap(p => Option(p.getImplementationVersion))
      .getOrElse("unknown")
    val observedScala = scala.util.Properties.versionNumberString
    val observedJava = System.getProperty("java.specification.version", "")
    require(spark.version.startsWith("4."), s"Unsupported Serverless Spark runtime: ${spark.version}")
    require(observedScala.startsWith("2.13."), s"Unsupported Serverless Scala runtime: $observedScala")
    require(observedJava == "17", s"Unsupported Serverless Java runtime: $observedJava")
    val observedConnectApi = DatabricksSession.getClass.getName
    System.err.println(
      s"SERVERLESS_RUNTIME_COMPATIBILITY environmentVersion=5 declaredDatabricksConnect=18.0.0 " +
        s"runtimeDatabricksConnect=$connectVersion observedConnectApi=$observedConnectApi " +
        s"spark=${spark.version} scala=$observedScala java=${System.getProperty("java.version")} " +
        s"transport=spark-connect platform=databricks-serverless compatibility=pass")

    // Zingg 0.7 obtains its session through SparkSession. Make the managed
    // Databricks session visible before invoking the ordinary Zingg main.
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)

    System.setProperty("zingg.native.transport", "spark-connect")
    System.setProperty("zingg.native.platform", "databricks-serverless")
    // Upstream Zingg's CLI client calls System.exit after cleanup.  Mark this
    // invocation so the overlaid client returns to the managed task instead.
    System.setProperty("zingg.native.managed", "true")
    val phase = phaseName(launch)
    var phaseSummaryEmitted = false
    def emitOrdinaryPhaseSummary(): Unit = {
      val isProbe = launch.differentialProbe || launch.modelProbe || launch.modelLoadProbe ||
        launch.modelCorruptionProbe || launch.graphProbe || launch.hashDifferentialProbe ||
        launch.graphBenchmark || launch.ldbcStress ||
        launch.numericDifferentialProbe || launch.dateArrayDifferentialProbe ||
        launch.stopWordsDifferentialProbe || launch.inputFormatProbe || launch.vectorProbe ||
        launch.blockingDifferentialProbe || launch.rowIdProbe || launch.fuzzyActionProbe ||
        launch.materializationFailureProbe || launch.materializationRecoveryProbe
      if (!isProbe && !phaseSummaryEmitted) {
        ai.zingg.nativebridge.NativeOperationProvider.fromSpark(spark, phase).emitPhaseSummary()
        phaseSummaryEmitted = true
      }
    }
    try {
      launch.interactiveInput.foreach { input =>
        // Serverless JAR tasks do not expose a user terminal. This explicit
        // opt-in is only for automated label/update-label probes; normal
        // Zingg invocation remains unchanged and still owns the Scanner path.
        System.setIn(new ByteArrayInputStream(
          (input.stripSuffix("\n") + "\n").getBytes(StandardCharsets.UTF_8)))
      }
      if (launch.modelCorruptionProbe) {
        ServerlessModelCorruptionProbe.run(spark)
      } else if (launch.modelProbe || launch.modelLoadProbe) {
        if (launch.modelLoadProbe) ServerlessModelProbe.loadAndPredict(spark)
        else ServerlessModelProbe.run(spark)
      } else if (launch.differentialProbe) {
        ServerlessDifferentialProbe.run(spark, launch.differentialRules)
      } else if (launch.graphProbe) {
        ServerlessGraphProbe.run(spark)
      } else if (launch.graphBenchmark) {
        ServerlessGraphBenchmark.run(spark)
      } else if (launch.ldbcStress) {
        LdbcGraphStress.run(spark, LdbcGraphStress.Config(
          System.getProperty("zingg.native.ldbc.dataset", "ldbc"),
          System.getProperty("zingg.native.ldbc.vertices"),
          System.getProperty("zingg.native.ldbc.edges"),
          Option(System.getProperty("zingg.native.ldbc.expected")).filter(_.nonEmpty)),
          System.getProperty("zingg.native.ldbc.output"))
      } else if (launch.hashDifferentialProbe) {
        ServerlessHashDifferentialProbe.run(spark, None)
      } else if (launch.numericDifferentialProbe) {
        ServerlessNumericDifferentialProbe.run(spark)
      } else if (launch.dateArrayDifferentialProbe) {
        ServerlessDateArrayDifferentialProbe.run(spark)
      } else if (launch.stopWordsDifferentialProbe) {
        ServerlessStopWordsDifferentialProbe.run(spark)
      } else if (launch.inputFormatProbe) {
        ServerlessInputFormatProbe.run(spark)
      } else if (launch.vectorProbe) {
        ServerlessVectorProbe.run(spark)
      } else if (launch.blockingDifferentialProbe) {
        ServerlessBlockingDifferentialProbe.run(spark)
      } else if (launch.rowIdProbe) {
        ServerlessRowIdProbe.run(spark)
      } else if (launch.fuzzyActionProbe) {
        ServerlessFuzzyActionProbe.run(spark)
      } else if (launch.materializationFailureProbe) {
        ServerlessMaterializationRecoveryProbe.leaveFailedSentinel(spark)
      } else if (launch.materializationRecoveryProbe) {
        ServerlessMaterializationRecoveryProbe.verifyRecovered(spark)
      } else {
        invokeMain(launch.delegateClass, launch.forwarded)
      }
      // The differential probes emit their own authoritative seam evidence;
      // ordinary phases get one managed summary here and, on failure, in the
      // finally block below.
      emitOrdinaryPhaseSummary()
    } finally {
      try emitOrdinaryPhaseSummary()
      catch {
        case summaryError: Throwable =>
          System.err.println(s"NATIVE_PHASE_SUMMARY_WARNING phase=$phase error=${summaryError.getMessage}")
      }
      // Databricks owns the Serverless session lifecycle. Clear only the
      // application bindings; never call SparkSession.stop() from a managed
      // task because that tears down the shared Spark Connect session.
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      sys.props.get("zingg.native.materialization.runRoot").foreach { root =>
        try ai.zingg.native.NativeMaterializationLifecycle.cleanup(root)
        catch {
          case cleanupError: Throwable =>
            System.err.println(s"NATIVE_MATERIALIZATION_CLEANUP_WARNING path=$root error=${cleanupError.getMessage}")
        }
      }
    }
  }

  private def phaseName(launch: LaunchArguments): String =
    if (launch.differentialProbe) "differential"
    else if (launch.graphProbe || launch.graphBenchmark) "graph-probe"
    else if (launch.hashDifferentialProbe) "hash-differential"
    else if (launch.numericDifferentialProbe) "numeric-differential"
    else if (launch.dateArrayDifferentialProbe) "date-array-differential"
    else if (launch.stopWordsDifferentialProbe) "stopwords-differential"
    else if (launch.inputFormatProbe) "input-format"
    else if (launch.vectorProbe) "vector-probe"
    else if (launch.blockingDifferentialProbe) "blocking-differential"
    else if (launch.rowIdProbe) "row-id"
    else launch.forwarded.sliding(2).collectFirst {
      case Array("--phase", value) => value
    }.getOrElse("unknown")

  private def applyNativeSettings(launch: LaunchArguments): Unit = {
    System.setProperty("zingg.native.mode", normalizeMode(launch.nativeMode))
    // Use one process-scoped correlation ID so rewrite records emitted by the
    // upstream boundary and the launcher phase summary describe the same run.
    System.setProperty("zingg.native.run.id", launch.runId.filter(_.nonEmpty).getOrElse(UUID.randomUUID().toString))
    launch.disabledRules.foreach(System.setProperty("zingg.native.disabled.rules", _))
    launch.planGuard.foreach(v => System.setProperty("zingg.native.plan.guard", normalizeBoolean(v)))
    launch.modelMaxIter.foreach(v =>
      System.setProperty("zingg.native.model.maxIter", v))
    launch.modelProbePath.foreach(v =>
      System.setProperty("zingg.native.model.probe.path", v))
    launch.modelQuickProbe.foreach(v =>
      System.setProperty("zingg.native.model.quickProbe", normalizeBoolean(v)))
    if (launch.forwarded.contains(NativeModelBoundedProbeFlag))
      System.setProperty("zingg.native.model.boundedProbe", "true")
    if (launch.forwarded.contains(NativeFuzzyActionsOnlyFlag))
      System.setProperty("zingg.native.fuzzy.actionsOnly", "true")
    optionValue(launch.forwarded, "--native-fuzzy-action-rows")
      .flatMap(_.toIntOption).filter(n => n > 0 && n <= 100)
      .foreach(n => System.setProperty("zingg.native.fuzzy.rows", n.toString))
    if (launch.forwarded.contains("--native-fuzzy-model-only"))
      System.setProperty("zingg.native.fuzzy.modelOnly", "true")
    launch.graphMaxIterations.foreach(v =>
      System.setProperty("zingg.native.graph.maxIterations", v))
    // Materialize only the narrow, already-computed similarity feature frame.
    // Polynomial terms remain public relational expressions and are expanded
    // after reread, avoiding repeated fuzzy-expression planning in gradients.
    val quickProbe = launch.modelQuickProbe.exists(v => normalizeBoolean(v) == "true")
    val runId = sys.props.getOrElse("zingg.native.run.id", UUID.randomUUID().toString)
    val runRoot = optionValue(launch.forwarded, "--zinggDir")
      .map(path => ai.zingg.native.NativeMaterializationLifecycle.runRoot(path, runId))
    val graphPath = launch.graphMaterializePath.map { path =>
      val configured = path.stripSuffix("/")
      val scopedRoot = runRoot.getOrElse(throw new IllegalArgumentException(
        "explicit graph materialization requires --zinggDir"))
      val scopedPrefix = scopedRoot.stripSuffix("/") + "/"
      require(configured == scopedRoot || configured.startsWith(scopedPrefix),
        s"explicit graph materialization must be beneath the native run root: $configured")
      s"$configured/$runId"
    }.orElse(runRoot.map(v => s"$v/graph"))
    graphPath.foreach(v => System.setProperty("zingg.native.graph.materializePath", v))
    val materializationRoot = runRoot
      .map(path => s"$path/base")
      .getOrElse(throw new IllegalArgumentException(
        "native materialization requires --zinggDir; refusing an unmanaged fallback path"))
    System.setProperty("zingg.native.model.materializePath", materializationRoot)
    System.setProperty("zingg.native.margin.materializePath", s"$materializationRoot/margins")
    System.setProperty("zingg.native.similarity.materializePath", s"$materializationRoot/similarity")
    runRoot.foreach(v => System.setProperty("zingg.native.materialization.runRoot", v))
  }

  private def normalizeMode(value: String): String = {
    val normalized = Option(value).getOrElse("STRICT").trim.toUpperCase
    normalized match {
      // The Serverless artifact intentionally exposes only the modes whose
      // execution contract is implemented by the public-expression adapter.
      // OFF/AUDIT require the classic Spark/GraphFrames/legacy-model path and
      // must fail at the launcher boundary instead of failing deep in a job.
      case "REWRITE" | "STRICT" => normalized
      case "OFF" | "AUDIT" => throw new IllegalArgumentException(
        s"Serverless does not support --native-mode $normalized; use REWRITE or STRICT")
      case other => throw new IllegalArgumentException(s"Unsupported --native-mode '$other'; expected REWRITE or STRICT")
    }
  }

  private def normalizeBoolean(value: String): String = Option(value).getOrElse("").trim.toLowerCase match {
    case "1" | "true" | "yes" | "on" => "true"
    case "0" | "false" | "no" | "off" => "false"
    case other => throw new IllegalArgumentException(s"Unsupported boolean '$other'; expected true/false")
  }

  private def optionValue(arguments: Array[String], flag: String): Option[String] = {
    arguments.sliding(2).collectFirst {
      case Array(name, value) if name == flag && value.trim.nonEmpty => value.trim
    }
  }

  private def resolveArguments(args: Array[String]): LaunchArguments = {
    var delegate: Option[String] = None
    var nativeMode: Option[String] = None
    var runId: Option[String] = None
    var disabledRules: Option[String] = None
    var planGuard: Option[String] = None
    var modelMaxIter: Option[String] = None
    var modelQuickProbe: Option[String] = None
    var graphMaxIterations: Option[String] = None
    var graphMaterializePath: Option[String] = None
    var differentialProbe = false
    var modelProbe = false
    var modelLoadProbe = false
    var modelCorruptionProbe = false
    var modelProbePath: Option[String] = None
    var graphProbe = false
    var graphBenchmark = false
    var ldbcStress = false
    var hashDifferentialProbe = false
    var numericDifferentialProbe = false
    var dateArrayDifferentialProbe = false
    var stopWordsDifferentialProbe = false
    var inputFormatProbe = false
    var vectorProbe = false
    var blockingDifferentialProbe = false
    var rowIdProbe = false
    var fuzzyActionProbe = false
    var materializationFailureProbe = false
    var materializationRecoveryProbe = false
    var differentialRules: Option[String] = None
    var interactiveInput: Option[String] = None
    val forwarded = ArrayBuffer.empty[String]
    var index = 0

    def valueFor(flag: String): String = {
      if (index + 1 >= args.length || args(index + 1).trim.isEmpty)
        throw new IllegalArgumentException(s"$flag requires a value")
      index += 1
      args(index).trim
    }

    while (index < args.length) {
      args(index) match {
        case DelegateFlag => delegate = Some(valueFor(DelegateFlag))
        case NativeModeFlag => nativeMode = Some(valueFor(NativeModeFlag))
        case NativeRunIdFlag => runId = Some(valueFor(NativeRunIdFlag))
        case NativeDisabledRulesFlag => disabledRules = Some(valueFor(NativeDisabledRulesFlag))
        case NativePlanGuardFlag => planGuard = Some(valueFor(NativePlanGuardFlag))
        case NativeModelMaxIterFlag => modelMaxIter = Some(valueFor(NativeModelMaxIterFlag))
        case NativeModelQuickProbeFlag => modelQuickProbe = Some(valueFor(NativeModelQuickProbeFlag))
        // These are adapter-only diagnostics. Consume them here so the
        // ordinary Zingg delegate never rejects native flags as unknown CLI
        // options; applyNativeSettings reads the same process properties.
        case NativeModelBoundedProbeFlag => System.setProperty("zingg.native.model.boundedProbe", "true")
        case NativeModelParityProbeFlag => System.setProperty("zingg.native.model.parityProbe", "true")
        case "--native-model-probe-features" => System.setProperty("zingg.native.model.probe.features", valueFor("--native-model-probe-features"))
        case NativeFuzzyActionsOnlyFlag => System.setProperty("zingg.native.fuzzy.actionsOnly", "true")
        case "--native-fuzzy-action-rows" => System.setProperty("zingg.native.fuzzy.rows", valueFor("--native-fuzzy-action-rows"))
        case "--native-fuzzy-action-rule" => System.setProperty("zingg.native.fuzzy.action.rule", valueFor("--native-fuzzy-action-rule"))
        case "--native-fuzzy-model-only" => System.setProperty("zingg.native.fuzzy.modelOnly", "true")
        case NativeGraphMaxIterationsFlag => graphMaxIterations = Some(valueFor(NativeGraphMaxIterationsFlag))
        case NativeGraphMaterializePathFlag => graphMaterializePath = Some(valueFor(NativeGraphMaterializePathFlag))
        case NativeGraphStrategyFlag => System.setProperty("zingg.native.graph.strategy", valueFor(NativeGraphStrategyFlag))
        case NativeDifferentialProbeFlag => differentialProbe = true
        case NativeModelProbeFlag => modelProbe = true
        case NativeModelLoadProbeFlag => modelLoadProbe = true
        case NativeModelCorruptionProbeFlag => modelCorruptionProbe = true
        case NativeModelProbePathFlag => modelProbePath = Some(valueFor(NativeModelProbePathFlag))
        case NativeGraphProbeFlag => graphProbe = true
        case NativeGraphBenchmarkFlag => graphBenchmark = true
        case NativeLdbcStressFlag => ldbcStress = true
        case NativeLdbcDatasetFlag => System.setProperty("zingg.native.ldbc.dataset", valueFor(NativeLdbcDatasetFlag))
        case NativeLdbcVerticesFlag => System.setProperty("zingg.native.ldbc.vertices", valueFor(NativeLdbcVerticesFlag))
        case NativeLdbcEdgesFlag => System.setProperty("zingg.native.ldbc.edges", valueFor(NativeLdbcEdgesFlag))
        case NativeLdbcExpectedFlag => System.setProperty("zingg.native.ldbc.expected", valueFor(NativeLdbcExpectedFlag))
        case NativeLdbcOutputFlag => System.setProperty("zingg.native.ldbc.output", valueFor(NativeLdbcOutputFlag))
        case NativeHashDifferentialProbeFlag => hashDifferentialProbe = true
        case NativeNumericDifferentialProbeFlag => numericDifferentialProbe = true
        case NativeDateArrayDifferentialProbeFlag => dateArrayDifferentialProbe = true
        case NativeStopWordsDifferentialProbeFlag => stopWordsDifferentialProbe = true
        case NativeInputFormatProbeFlag => inputFormatProbe = true
        case NativeVectorProbeFlag => vectorProbe = true
        case NativeBlockingDifferentialProbeFlag => blockingDifferentialProbe = true
        case NativeRowIdProbeFlag => rowIdProbe = true
        case NativeFuzzyActionProbeFlag => fuzzyActionProbe = true
        case NativeMaterializationFailureProbeFlag => materializationFailureProbe = true
        case NativeMaterializationRecoveryProbeFlag => materializationRecoveryProbe = true
        case NativeDifferentialRulesFlag => differentialRules = Some(valueFor(NativeDifferentialRulesFlag))
        case NativeInteractiveInputFlag => interactiveInput = Some(valueFor(NativeInteractiveInputFlag))
        case other => forwarded += other
      }
      index += 1
    }

    val propertyDelegate = Option(System.getProperty("zingg.native.delegate.main")).map(_.trim).filter(_.nonEmpty)
    val resolvedDelegate = delegate.orElse(propertyDelegate).orElse {
      if (differentialProbe || modelProbe || modelLoadProbe || modelCorruptionProbe || graphProbe || graphBenchmark || ldbcStress || hashDifferentialProbe || numericDifferentialProbe || dateArrayDifferentialProbe || stopWordsDifferentialProbe || inputFormatProbe || vectorProbe || blockingDifferentialProbe || rowIdProbe || fuzzyActionProbe || materializationFailureProbe || materializationRecoveryProbe) Some("") else None
    }.getOrElse {
      throw new IllegalArgumentException(
        s"Missing Zingg delegate main class. Supply '$DelegateFlag <class>' or -Dzingg.native.delegate.main=<class>.")
    }
    val propertyMode = Option(System.getProperty("zingg.native.mode")).map(_.trim).filter(_.nonEmpty)

    LaunchArguments(
      resolvedDelegate,
      forwarded.toArray,
      nativeMode.orElse(propertyMode).getOrElse("STRICT"),
      runId,
      disabledRules,
      planGuard,
      modelMaxIter,
      modelQuickProbe,
      graphMaxIterations,
      graphMaterializePath,
      differentialProbe,
      modelProbe,
      modelLoadProbe,
      modelCorruptionProbe,
      modelProbePath,
      graphProbe,
      graphBenchmark,
      ldbcStress,
      hashDifferentialProbe,
      numericDifferentialProbe,
      dateArrayDifferentialProbe,
      stopWordsDifferentialProbe,
      inputFormatProbe,
      vectorProbe,
      blockingDifferentialProbe,
      rowIdProbe,
      fuzzyActionProbe,
      materializationFailureProbe,
      materializationRecoveryProbe,
      differentialRules,
      interactiveInput)
  }

  /** Support both ordinary Java static main methods and Scala object mains. */
  private def invokeMain(className: String, args: Array[String]): Unit = {
    try {
      val clazz = Class.forName(className)
      val method = clazz.getMethod("main", classOf[Array[String]])
      invoke(method, null, args)
    } catch {
      case _: ClassNotFoundException if !className.endsWith("$") => invokeScalaObjectMain(className, args)
      case _: NoSuchMethodException => invokeScalaObjectMain(className, args)
    }
  }

  private def invokeScalaObjectMain(className: String, args: Array[String]): Unit = {
    val objectClass = Class.forName(if (className.endsWith("$")) className else className + "$")
    val module = objectClass.getField("MODULE$").get(null)
    val method = objectClass.getMethod("main", classOf[Array[String]])
    invoke(method, module, args)
  }

  private def invoke(method: Method, receiver: AnyRef, args: Array[String]): Unit = {
    try method.invoke(receiver, args.asInstanceOf[Object])
    catch {
      case e: InvocationTargetException =>
        val cause = Option(e.getCause).getOrElse(e)
        cause match {
          case runtime: RuntimeException => throw runtime
          case error: Error => throw error
          case other => throw new IllegalStateException("Delegated Zingg main failed", other)
        }
    }
  }
}
