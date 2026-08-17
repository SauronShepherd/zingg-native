package ai.zingg.nativebridge

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** Java-source-compatible facade over the Scala native provider. */
final class NativeOperationProvider private (private val delegate: ai.zingg.native.NativeOperationProvider) {
  def mode: String = delegate.mode
  def similarityByZinggName(
      input: Dataset[Row],
      zinggFunctionName: String,
      leftColumn: String,
      rightColumn: String,
      outputColumn: String): Dataset[Row] =
    delegate.similarityByZinggName(input, zinggFunctionName, leftColumn, rightColumn, outputColumn)
}

object NativeOperationProvider {
  def fromSpark(spark: SparkSession, phase: String): NativeOperationProvider =
    new NativeOperationProvider(ai.zingg.native.NativeOperationProvider.fromSpark(spark, phase))
}
