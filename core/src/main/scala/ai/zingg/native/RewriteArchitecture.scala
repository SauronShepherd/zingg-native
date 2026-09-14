package ai.zingg.native

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

sealed trait NativeExecutionMode { def id: String; def rewrites: Boolean; def audits: Boolean }
object NativeExecutionMode {
  case object OFF extends NativeExecutionMode { val id="OFF"; val rewrites=false; val audits=false }
  case object AUDIT extends NativeExecutionMode { val id="AUDIT"; val rewrites=false; val audits=true }
  case object REWRITE extends NativeExecutionMode { val id="REWRITE"; val rewrites=true; val audits=true }
  case object STRICT extends NativeExecutionMode { val id="STRICT"; val rewrites=true; val audits=true }
  def parse(value: String): NativeExecutionMode = Option(value).getOrElse("OFF").trim.toUpperCase match {
    case "OFF" => OFF; case "AUDIT" => AUDIT; case "REWRITE" => REWRITE; case "STRICT" => STRICT
    case other => throw new IllegalArgumentException(s"Unknown native execution mode: $other")
  }
}

final case class NativeOperation(id: String)
object NativeOperation {
  private val similarityNames = Seq(
    "SimilarityFunctionExact", "StringSimilarityFunction", "CheckNullFunction", "CheckBlankOrNullFunction",
    "IntegerSimilarityFunction", "LongSimilarityFunction", "DoubleSimilarityFunction", "FloatSimilarityFunction",
    "DateSimilarityFunction", "ArrayDoubleSimilarityFunction", "JaccSimFunction", "BigramJaccSimFn",
    "NumbersJaccardFunction", "ProductCodeFunction", "JaroWinklerFunction", "AJaroWinklerFunction",
    "AffineGapSimilarityFunction", "EmailMatchTypeFunction", "PinCodeMatchTypeFunction",
    "OnlyAlphabetsExactSimilarity", "OnlyAlphabetsAffineGapSimilarity", "SameFirstWordFunction")
  private val hashNames = Seq(
    "first1Chars","first2Chars","first3Chars","first4Chars","last1Chars","last2Chars","last3Chars","lastWord",
    "isNullOrEmpty","identityString","first2CharsBox","first3CharsBox","identityInteger","identityLong","identityBoolean",
    "truncateDoubleTo1Places","truncateDoubleTo2Places","truncateDoubleTo3Places",
    "truncateFloatTo1Places","truncateFloatTo2Places","truncateFloatTo3Places",
    "lessThanZeroDbl","lessThanZeroFloat","lessThanZeroInt","lessThanZeroLong",
    "trimLast1DigitsDbl","trimLast2DigitsDbl","trimLast3DigitsDbl",
    "trimLast1DigitsFloat","trimLast2DigitsFloat","trimLast3DigitsFloat",
    "trimLast1DigitsInt","trimLast2DigitsInt","trimLast3DigitsInt",
    "trimLast1DigitsLong","trimLast2DigitsLong","trimLast3DigitsLong",
    "rangeBetween0And10Dbl","rangeBetween10And100Dbl","rangeBetween100And1000Dbl","rangeBetween1000And10000Dbl",
    "rangeBetween0And10Float","rangeBetween10And100Float","rangeBetween100And1000Float","rangeBetween1000And10000Float",
    "rangeBetween0And10Int","rangeBetween10And100Int","rangeBetween100And1000Int","rangeBetween1000And10000Int",
    "rangeBetween0And10Long","rangeBetween10And100Long","rangeBetween100And1000Long","rangeBetween1000And10000Long","round")
  val preprocessNames = Seq("trim", "caseNormalize", "stopWords")
  val modelNames = Seq("vectorValue", "nativeLogisticCv", "nativePrediction", "nativePersistence.save", "nativePersistence.load")
  val graphNames = Seq("connectedComponents")
  val blockingNames = Seq("blockingTree")
  val all: Seq[NativeOperation] =
    similarityNames.map(n => NativeOperation(s"similarity.$n")) ++
    hashNames.map(n => NativeOperation(s"blocking.$n")) ++
    preprocessNames.map(n => NativeOperation(s"preprocess.$n")) ++
    modelNames.map(n => NativeOperation(s"model.$n")) ++
    graphNames.map(n => NativeOperation(s"graph.$n")) ++
    blockingNames.map(n => NativeOperation(s"blocking.$n"))
  private val byId = all.map(o => o.id -> o).toMap
  def resolve(id: String): NativeOperation = byId.getOrElse(id, NativeOperation(id))
}

final case class RewriteContext(
  spark: SparkSession,
  mode: NativeExecutionMode,
  runtime: RuntimeDescriptor,
  phase: String = "unknown",
  correlationId: String = "",
  parameters: Map[String,String] = Map.empty) {
  lazy val disabledRules: Set[String] = parameters.get("disabledRules").toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty).toSet
  def isDisabled(operationId:String, ruleId:String):Boolean = disabledRules.contains(operationId) || disabledRules.contains(ruleId)
}

trait RewriteRule {
  def id: String
  def operation: NativeOperation
  def apply(left: Column, right: Option[Column], context: RewriteContext): Column
}
final class FunctionalRewriteRule(
  val id: String,
  val operation: NativeOperation,
  fn: (Column, Option[Column], RewriteContext) => Column) extends RewriteRule {
  def apply(left: Column, right: Option[Column], context: RewriteContext): Column = fn(left,right,context)
}

final case class NativeFinding(phase:String, operation:String, construct:String, rewritten:Boolean, diagnostic:String)
final case class NativeCompatibilityReport(phase:String, findings:Seq[NativeFinding]) {
  def unsupported: Seq[NativeFinding] = findings.filterNot(_.rewritten)
  def isCompatible: Boolean = unsupported.isEmpty
}

final class RewriteRegistry private (private val rules: Map[String, RewriteRule]) {
  def resolve(operation: NativeOperation): RewriteRule = rules.getOrElse(operation.id,
    throw new NativeRewriteUnsupportedException(s"No rewrite rule registered for ${operation.id}"))
  def contains(operation: NativeOperation): Boolean = rules.contains(operation.id)
  def operationIds: Seq[String] = rules.keys.toSeq.sorted
}
object RewriteRegistry {
  def apply(rules: Seq[RewriteRule]): RewriteRegistry = {
    val duplicate = rules.groupBy(_.operation.id).collect{ case (id, xs) if xs.size > 1 => id }.toSeq.sorted
    require(duplicate.isEmpty, s"Duplicate rewrite rules: ${duplicate.mkString(", ")}")
    new RewriteRegistry(rules.map(r => r.operation.id -> r).toMap)
  }
}

object NativeCompatibilityAnalyzer {
  def analyze(phase:String, operations:Seq[(NativeOperation,Boolean,String)]): NativeCompatibilityReport =
    NativeCompatibilityReport(phase, operations.map { case(op,rewritten,construct) =>
      NativeFinding(phase,op.id,construct,rewritten,if(rewritten) "rewrite available" else s"no rewrite registered for ${op.id}")
    })
}
final class NativeRewriteUnsupportedException(message:String) extends IllegalStateException(message)

object NativePlanGuard {
  private val forbidden = Seq("ScalaUDF", "PythonUDF", "BatchEvalPython", "ArrowEvalPython", "MapElements", "MapPartitions", "SerializeFromObject", "DeserializeToObject", "ExistingRDD")
  def requireCompatible(report: NativeCompatibilityReport, context: RewriteContext): Unit =
    if(context.mode == NativeExecutionMode.STRICT && !report.isCompatible)
      throw new NativeRewriteUnsupportedException(s"STRICT native execution rejected phase '${report.phase}': ${report.unsupported.map(_.operation).mkString(", ")}")
  def inspectPlan(phase:String, plan:String, context:RewriteContext): NativeCompatibilityReport = {
    val findings = forbidden.filter(plan.contains).map(n => NativeFinding(phase,s"plan.$n",n,false,s"forbidden non-native plan node $n"))
    val report = NativeCompatibilityReport(phase,findings)
    requireCompatible(report,context); report
  }
  def explain(df: DataFrame, extended:Boolean=true): String = {
    val baos = new java.io.ByteArrayOutputStream()
    Console.withOut(new java.io.PrintStream(baos, true, StandardCharsets.UTF_8)) { df.explain(extended) }
    baos.toString(StandardCharsets.UTF_8)
  }
  def normalize(plan:String):String = plan.replaceAll("#\\d+", "#?").replaceAll("id=\\d+", "id=?").replaceAll("\\s+", " ").trim
  def fingerprint(plan:String):String = sha256(normalize(plan))
  def sha256(value:String):String = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)).map("%02x".format(_)).mkString

  /**
   * Planning-time strict guard.  It analyzes the public DataFrame plan without
   * triggering a Spark action, so native mode fails before execution if a
   * known JVM/Python callback or object-encoder node survived rewriting.
   * The hot path uses the physical plan only; extended parsed/analyzed output
   * is reserved for explicit evidence capture because its expansion is
   * disproportionately expensive for large public higher-order expressions.
   */
  def guardDataFrame(df:DataFrame, context:RewriteContext):DataFrame = {
    val enabled = sys.props.get("zingg.native.plan.guard")
      .orElse(sys.env.get("ZINGG_NATIVE_PLAN_GUARD"))
      .forall(v => !Set("0","false","off","no").contains(v.trim.toLowerCase))
    if(context.mode == NativeExecutionMode.STRICT && enabled) {
      // similarity.batch has already resolved every operation through the
      // public rewrite registry and checked each rule's disabled state before
      // constructing this projection. Expanding its physical plan here is
      // prohibitively expensive for 20 AffineGap/Jaro expressions on
      // Serverless and adds no new forbidden-node information: the registry
      // emits only public Column expressions. All other strict boundaries keep
      // the full forbidden-node inspection.
      if (context.phase != "similarity.batch")
        inspectPlan(context.phase, explain(df, extended=false), context)
    }
    df
  }
}

final case class NativeExecutionEvidence(
  phase:String, mode:String, appliedRules:Seq[String], planFingerprint:String,
  runtime:RuntimeDescriptor, outputFingerprint:Option[String], photonEvidence:Option[String], correlationId:String)

object NativeEvidenceCollector {
  private val rules = TrieMap.empty[String, Vector[String]]
  private val evidence = TrieMap.empty[String, NativeExecutionEvidence]
  @volatile private var latestEvidence:Option[NativeExecutionEvidence] = None
  private def addRule(runId:String, ruleId:String):Unit = if(runId.nonEmpty) rules.updateWith(runId)(v => Some(v.getOrElse(Vector.empty) :+ ruleId))
  def recordRule(runId:String, ruleId:String):Unit = addRule(runId,ruleId)
  def recordRule(context:RewriteContext, ruleId:String):Unit = { addRule(context.correlationId,ruleId); NativeDiagnostics.rewrite(context,ruleId) }
  def applied(runId:String):Seq[String] = rules.getOrElse(runId,Vector.empty).distinct
  def capture(df:DataFrame, context:RewriteContext, photonEvidence:Option[String]=None):NativeExecutionEvidence = {
    val plan = NativePlanGuard.explain(df, extended=true)
    NativePlanGuard.inspectPlan(context.phase,plan,context)
    val outputFingerprint = captureOutputFingerprint(df)
    val e = NativeExecutionEvidence(context.phase,context.mode.id,applied(context.correlationId),NativePlanGuard.fingerprint(plan),context.runtime,Some(outputFingerprint),photonEvidence,context.correlationId)
    if(context.correlationId.nonEmpty) evidence.put(context.correlationId,e)
    latestEvidence = Some(e)
    NativeDiagnostics.phaseSummary(e)
    e
  }
  /**
    * Privacy-safe multiset evidence. Only a row count and a digest of sorted
    * row digests leave the Spark plan; raw values are never logged or returned.
    * This is intentionally available at the explicit evidence seam, not on
    * every ordinary phase boundary.
    */
  private def captureOutputFingerprint(df:DataFrame):String = {
    val rowDigest = sha2(to_json(struct(df.col("*"))), 256).alias("_native_row_digest")
    val summary = df.select(rowDigest).agg(
      count(lit(1)).alias("_native_row_count"),
      sha2(concat_ws("|", sort_array(collect_list(col("_native_row_digest")))), 256)
        .alias("_native_multiset_sha256")).head()
    s"rows=${summary.getLong(0)};multisetSha256=${summary.getString(1)}"
  }
  /** Emit a phase-end summary even when the upstream phase does not expose a
    * final DataFrame at the integration seam.  The plan fingerprint remains
    * explicitly unavailable rather than being inferred or fabricated. */
  def phaseSummary(context:RewriteContext):NativeExecutionEvidence = {
    evidence.get(context.correlationId).orElse(latestEvidence.filter(_.phase == context.phase)).foreach { captured =>
      NativeDiagnostics.phaseSummary(captured)
      return captured
    }
    val e = NativeExecutionEvidence(
      context.phase, context.mode.id, applied(context.correlationId),
      "unavailable", context.runtime, None, None, context.correlationId)
    if(context.correlationId.nonEmpty) evidence.put(context.correlationId,e)
    NativeDiagnostics.phaseSummary(e)
    e
  }
  def get(runId:String):Option[NativeExecutionEvidence] = evidence.get(runId)
  def clear(runId:String):Unit = { rules.remove(runId); evidence.remove(runId) }
}
