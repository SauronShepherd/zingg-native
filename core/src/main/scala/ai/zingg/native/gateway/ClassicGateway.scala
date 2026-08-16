package ai.zingg.native.gateway

import ai.zingg.native.Core
import org.apache.spark.sql.DataFrame

/** Deliberately small Java/Py4J-friendly gateway; Spark dependencies stay provided. */
class ClassicGateway {
  def libraryVersion: String = Core.libraryVersion
  def protocolVersion: String = Core.protocolVersion
  def sparkVersion(df: DataFrame): String = df.sparkSession.version
  def supportedOperations: Array[String] = Array("EXACT_SIMILARITY", "JACCARD_SIMILARITY")
  def transform(df: DataFrame, operationId: String, left: String, right: String, output: String): DataFrame = Core.transform(df, operationId, left, right, output)
}
