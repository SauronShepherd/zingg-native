package ai.zingg.native

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/** Runtime metadata only; Photon/native truth is supplied later by query evidence. */
final case class RuntimeDescriptor(
    sparkVersion: String,
    scalaVersion: String,
    photon: Option[Boolean] = None)

/**
 * Small public-expression kernel used by the Zingg integration provider and
 * the optional Classic/Py4J gateway.  This is deliberately not a second Zingg
 * phase engine: upstream Zingg remains responsible for training, matching,
 * linking, persistence and phase orchestration.
 */
object Core {
  val libraryVersion = "0.3.0-SNAPSHOT"
  val protocolVersion = "1"

  private val operationAliases = Map(
    "EXACT_SIMILARITY" -> "similarity.SimilarityFunctionExact",
    "JACCARD_SIMILARITY" -> "similarity.JaccSimFunction",
    "JARO_SIMILARITY" -> "similarity.JaroWinklerFunction",
    "TRIM" -> "preprocess.trim",
    "CASE_NORMALIZE" -> "preprocess.caseNormalize")

  def normalizeOperation(operationId: String): String =
    operationAliases.getOrElse(operationId, operationId)

  private def executionContext(spark: SparkSession, phase: String): RewriteContext = {
    val mode = sys.props.get("zingg.native.mode")
      .orElse(sys.env.get("ZINGG_NATIVE_MODE")).getOrElse("STRICT")
    val runId = sys.props.get("zingg.native.run.id")
      .orElse(sys.env.get("ZINGG_NATIVE_RUN_ID")).getOrElse(java.util.UUID.randomUUID().toString)
    val disabled = sys.props.get("zingg.native.disabled.rules")
      .orElse(sys.env.get("ZINGG_NATIVE_DISABLED_RULES")).getOrElse("")
    RewriteContext(
      spark,
      NativeExecutionMode.parse(mode),
      RuntimeDescriptor(spark.version, "2.13"),
      phase,
      runId,
      Map("disabledRules" -> disabled))
  }

  /** Convenience gateway for one expression; production Zingg calls rewrite directly through the provider. */
  def transform(
      df: DataFrame,
      operationId: String,
      left: String,
      right: String,
      output: String): DataFrame =
    rewrite(df, normalizeOperation(operationId), left, Option(right), output,
      executionContext(df.sparkSession, "gateway.transform"))

  /** Execute one registered public-expression rewrite with explicit policy. */
  def rewrite(
      df: DataFrame,
      operationId: String,
      left: String,
      right: Option[String],
      output: String,
      context: RewriteContext,
      registry: RewriteRegistry = NativeRewriteRegistry.default): DataFrame = {
    val normalized = normalizeOperation(operationId)
    val operation = NativeOperation.resolve(normalized)
    val rule = registry.resolve(operation)
    if (context.isDisabled(operation.id, rule.id))
      throw new NativeRewriteUnsupportedException(
        s"Native rewrite disabled for ${operation.id} (${rule.id})")
    NativeEvidenceCollector.recordRule(context, rule.id)
    if (!context.mode.rewrites) df
    else NativePlanGuard.guardDataFrame(
      df.withColumn(output, rule(col(left), right.map(col), context)), context)
  }

  /**
   * Apply independent column rewrites as one public projection. Every rule is
   * still resolved, observable, and individually disable-able; batching only
   * avoids repeatedly nesting the plan and repeatedly explaining it at the
   * similarity-feature boundary.
   */
  def rewriteColumns(
      df: DataFrame,
      operations: Seq[(String, String, Option[String], String)],
      context: RewriteContext,
      registry: RewriteRegistry = NativeRewriteRegistry.default): DataFrame = {
    if (operations.isEmpty) return df
    val additions: Seq[(String, Column)] = operations.map { case (operationId, left, right, output) =>
      val normalized = normalizeOperation(operationId)
      val operation = NativeOperation.resolve(normalized)
      val rule = registry.resolve(operation)
      if (context.isDisabled(operation.id, rule.id))
        throw new NativeRewriteUnsupportedException(
          s"Native rewrite disabled for ${operation.id} (${rule.id})")
      NativeEvidenceCollector.recordRule(context, rule.id)
      output -> rule(col(left), right.map(col), context)
    }
    // withColumns is public Spark SQL/DataFrame API and appends the generated
    // columns without requesting remote schema metadata or resolving a
    // wildcard projection through managed Spark Connect.
    val projected = df.withColumns(additions.toMap)
    NativeDiagnostics.planGuard(context, "start", s"operations=${operations.size}")
    val guarded = NativePlanGuard.guardDataFrame(projected, context)
    NativeDiagnostics.planGuard(context, "complete", s"operations=${operations.size}")
    guarded
  }

  /** Apply a registered unary preprocessing rewrite to existing columns. */
  def preprocess(df: DataFrame, operationId: String, columns: Seq[String]): DataFrame = {
    require(columns.nonEmpty, "columns must contain at least one field")
    val missing = columns.distinct.filterNot(df.columns.contains)
    require(missing.isEmpty, s"Unknown preprocessing columns: ${missing.mkString(", ")}")
    val normalized = normalizeOperation(operationId)
    val context = executionContext(df.sparkSession, "gateway.preprocess")
    columns.foldLeft(df)((current, field) => rewrite(current, normalized, field, None, field, context))
  }
}
