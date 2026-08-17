package ai.zingg.native.gateway

import ai.zingg.native.Core
import org.apache.spark.sql.DataFrame
import scala.jdk.CollectionConverters._

/** Deliberately small Java/Py4J-friendly gateway; Spark dependencies stay provided. */
class ClassicGateway {
  def libraryVersion: String = Core.libraryVersion
  def protocolVersion: String = Core.protocolVersion
  def capabilityMetadata: String = "shared-core;EXACT_SIMILARITY;JACCARD_SIMILARITY;JARO_SIMILARITY;phase-parity-not-certified"
  def sparkVersion(df: DataFrame): String = df.sparkSession.version
  def supportedOperations: Array[String] = Array("EXACT_SIMILARITY", "JACCARD_SIMILARITY", "JARO_SIMILARITY")
  def transform(df: DataFrame, operationId: String, left: String, right: String, output: String): DataFrame = Core.transform(df, operationId, left, right, output)
  def findTrainingData(df: DataFrame, idColumn: String, keys: java.util.List[String]): DataFrame =
    Core.findTrainingData(df, idColumn, keys.asScala.toSeq)
  def label(df: DataFrame, threshold: Double): DataFrame = Core.label(df, threshold)
}
