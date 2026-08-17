package ai.zingg.native

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Java-friendly integration seam for the upstream Zingg Spark implementation.
  *
  * Upstream transformers can obtain this provider from their Spark context and
  * call it at semantic operation boundaries. User applications do not need to
  * call this class directly.
  */
final class NativeOperationProvider private (val spark: SparkSession, val context: RewriteContext) {
  def mode: String = context.mode.id

  def similarity(
      input: Dataset[Row],
      operationId: String,
      leftColumn: String,
      rightColumn: String,
      outputColumn: String): Dataset[Row] = {
    val result = Core.rewrite(input.toDF(), operationId, leftColumn, Some(rightColumn), outputColumn, context)
    result.asInstanceOf[Dataset[Row]]
  }

  /** Resolve the upstream SimFunction name without exposing JVM class names. */
  def similarityByZinggName(
      input: Dataset[Row],
      zinggFunctionName: String,
      leftColumn: String,
      rightColumn: String,
      outputColumn: String): Dataset[Row] = {
    val normalized = Option(zinggFunctionName).getOrElse("").toLowerCase
    val operation =
      if (normalized.contains("jacc")) "similarity.jaccard"
      else if (normalized.contains("jaro")) "similarity.jaro"
      else if (normalized.contains("exact")) "similarity.exact"
      else throw new NativeRewriteUnsupportedException(
        s"No native similarity mapping for upstream function '$zinggFunctionName'")
    similarity(input, operation, leftColumn, rightColumn, outputColumn)
  }

  def preprocess(input: Dataset[Row], operationId: String, columns: Array[String]): Dataset[Row] = {
    val result = Core.preprocess(input.toDF(), operationId, columns.toSeq)
    result.asInstanceOf[Dataset[Row]]
  }

  def analyze(phase: String, operationId: String, construct: String): NativeCompatibilityReport = {
    val operation = NativeOperation.resolve(operationId)
    NativeCompatibilityAnalyzer.analyze(phase, Seq((operation, NativeRewriteRegistry.default.contains(operation), construct)))
  }

  def guard(report: NativeCompatibilityReport): Unit = NativePlanGuard.requireCompatible(report, context)
}

object NativeOperationProvider {
  def fromSpark(spark: SparkSession, phase: String): NativeOperationProvider = {
    // Serverless-safe activation: use a supported application argument or
    // environment variable, never an arbitrary Spark configuration key.
    val modeValue = sys.props.get("zingg.native.mode")
      .orElse(sys.env.get("ZINGG_NATIVE_MODE"))
      .getOrElse("OFF")
    val correlationId = sys.props.get("zingg.native.run.id")
      .orElse(sys.env.get("ZINGG_NATIVE_RUN_ID"))
      .getOrElse("")
    val mode = NativeExecutionMode.parse(modeValue)
    new NativeOperationProvider(
      spark,
      RewriteContext(spark, mode, RuntimeDescriptor(spark.version, "2.13"), phase, correlationId))
  }
}
