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
    val mode = NativeExecutionMode.parse(spark.conf.get("zingg.native.mode", "OFF"))
    val correlationId = spark.conf.get("zingg.native.run.id", "")
    new NativeOperationProvider(
      spark,
      RewriteContext(spark, mode, RuntimeDescriptor(spark.version, "2.13"), phase, correlationId))
  }
}
