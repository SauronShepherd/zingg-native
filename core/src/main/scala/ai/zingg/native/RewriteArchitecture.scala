package ai.zingg.native

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** Execution policy for the native rewrite layer. */
sealed trait NativeExecutionMode { def id: String }
object NativeExecutionMode {
  case object OFF extends NativeExecutionMode { val id = "OFF" }
  case object AUDIT extends NativeExecutionMode { val id = "AUDIT" }
  case object REWRITE extends NativeExecutionMode { val id = "REWRITE" }
  case object STRICT extends NativeExecutionMode { val id = "STRICT" }

  def parse(value: String): NativeExecutionMode = value.trim.toUpperCase match {
    case "OFF" => OFF
    case "AUDIT" => AUDIT
    case "REWRITE" => REWRITE
    case "STRICT" => STRICT
    case other => throw new IllegalArgumentException(s"Unknown native execution mode: $other")
  }
}

/** Stable semantic IDs; these are not JVM implementation class names. */
sealed trait NativeOperation { def id: String }
object NativeOperation {
  case object ExactSimilarity extends NativeOperation { val id = "similarity.exact" }
  case object JaccardSimilarity extends NativeOperation { val id = "similarity.jaccard" }
  case object JaroSimilarity extends NativeOperation { val id = "similarity.jaro" }
  case object Trim extends NativeOperation { val id = "preprocess.trim" }
  case object CaseNormalize extends NativeOperation { val id = "preprocess.case_normalize" }
  // Inventoried upstream boundaries. They are intentionally unresolved until
  // a semantic oracle proves a public-expression replacement.
  case object Hash extends NativeOperation { val id = "blocking.hash" }
  case object BlockingTree extends NativeOperation { val id = "blocking.tree" }
  case object StopWords extends NativeOperation { val id = "preprocess.stopwords" }
  case object VectorExtraction extends NativeOperation { val id = "model.vector_extraction" }
  case object GraphLink extends NativeOperation { val id = "link.connected_components" }
  case object First1Chars extends NativeOperation { val id = "blocking.first1Chars" }
  case object First2Chars extends NativeOperation { val id = "blocking.first2Chars" }
  case object First3Chars extends NativeOperation { val id = "blocking.first3Chars" }
  case object First4Chars extends NativeOperation { val id = "blocking.first4Chars" }
  case object Last1Chars extends NativeOperation { val id = "blocking.last1Chars" }
  case object Last2Chars extends NativeOperation { val id = "blocking.last2Chars" }
  case object Last3Chars extends NativeOperation { val id = "blocking.last3Chars" }

  val all: Seq[NativeOperation] = Seq(ExactSimilarity, JaccardSimilarity, JaroSimilarity, Trim, CaseNormalize,
    Hash, BlockingTree, StopWords, VectorExtraction, GraphLink, First1Chars, First2Chars, First3Chars,
    First4Chars, Last1Chars, Last2Chars, Last3Chars)
  private val byId = all.map(op => op.id -> op).toMap
  def resolve(id: String): NativeOperation = byId.getOrElse(id, throw new IllegalArgumentException(s"Unknown native operation: $id"))
}

final case class RewriteContext(
    spark: SparkSession,
    mode: NativeExecutionMode,
    runtime: RuntimeDescriptor,
    phase: String = "unknown",
    correlationId: String = "")

/** One semantic rewrite rule. Implementations must use public Spark APIs. */
trait RewriteRule {
  def id: String
  def operation: NativeOperation
  def apply(left: Column, right: Option[Column], context: RewriteContext): Column
}

final case class NativeFinding(
    phase: String,
    operation: String,
    construct: String,
    rewritten: Boolean,
    diagnostic: String)

final case class NativeCompatibilityReport(phase: String, findings: Seq[NativeFinding]) {
  def unsupported: Seq[NativeFinding] = findings.filterNot(_.rewritten)
  def isCompatible: Boolean = unsupported.isEmpty
}

/** Deterministic rule lookup; no implicit approximation or fallback. */
final class RewriteRegistry private (private val rules: Map[String, RewriteRule]) {
  def resolve(operation: NativeOperation): RewriteRule =
    rules.getOrElse(operation.id, throw new IllegalArgumentException(s"No rewrite rule registered for ${operation.id}"))
  def contains(operation: NativeOperation): Boolean = rules.contains(operation.id)
  def operationIds: Seq[String] = rules.keys.toSeq.sorted
}

object RewriteRegistry {
  def empty: RewriteRegistry = new RewriteRegistry(Map.empty)
  def apply(rules: Seq[RewriteRule]): RewriteRegistry = {
    val grouped = rules.groupBy(_.operation.id)
    val duplicate = grouped.collect { case (id, values) if values.size > 1 => id }.toSeq.sorted
    require(duplicate.isEmpty, s"Duplicate rewrite rules: ${duplicate.mkString(", ")}")
    new RewriteRegistry(rules.map(rule => rule.operation.id -> rule).toMap)
  }
}

object NativeCompatibilityAnalyzer {
  def analyze(phase: String, operations: Seq[(NativeOperation, Boolean, String)]): NativeCompatibilityReport =
    NativeCompatibilityReport(phase, operations.map { case (operation, rewritten, construct) =>
      NativeFinding(phase, operation.id, construct, rewritten,
        if (rewritten) "rewrite available" else s"no rewrite registered for ${operation.id}")
    })
}

final class NativeRewriteUnsupportedException(message: String) extends IllegalStateException(message)

object NativePlanGuard {
  def requireCompatible(report: NativeCompatibilityReport, context: RewriteContext): Unit = {
    if (context.mode == NativeExecutionMode.STRICT && !report.isCompatible) {
      val details = report.unsupported.map(f => s"${f.operation} (${f.construct})").mkString(", ")
      throw new NativeRewriteUnsupportedException(
        s"STRICT native execution rejected phase '${report.phase}' for ${context.runtime.sparkVersion}: $details")
    }
  }
}

final case class NativeExecutionEvidence(
    phase: String,
    mode: String,
    appliedRules: Seq[String],
    planFingerprint: String,
    runtime: RuntimeDescriptor,
    outputFingerprint: Option[String],
    photonEvidence: Option[String])
