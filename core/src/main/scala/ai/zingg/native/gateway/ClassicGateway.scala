package ai.zingg.native.gateway

import ai.zingg.native.{ArtifactSchema, Core}
import org.apache.spark.sql.DataFrame
import scala.jdk.CollectionConverters._

/** Deliberately small Java/Py4J-friendly gateway; Spark dependencies stay provided. */
class ClassicGateway {
  def libraryVersion: String = Core.libraryVersion
  def protocolVersion: String = Core.protocolVersion
  def modelArtifactSchemaVersion: Int = ArtifactSchema.currentVersion
  def blockingTreeArtifactSchemaVersion: Int = ArtifactSchema.currentVersion
  def capabilityMetadata: String = "shared-core;EXACT_SIMILARITY;JACCARD_SIMILARITY;JARO_SIMILARITY;CLASSIC_FIND_TRAINING_DATA;CLASSIC_LABEL;CLASSIC_UPDATE_LABEL;model-artifact-schema-v1;blocking-tree-artifact-schema-v1;phase-parity-not-certified"
  def sparkVersion(df: DataFrame): String = df.sparkSession.version
  def supportedOperations: Array[String] = Array("EXACT_SIMILARITY", "JACCARD_SIMILARITY", "JARO_SIMILARITY")
  def supportedPhases: Array[String] = Array("findTrainingData", "label", "updateLabel")
  def transform(df: DataFrame, operationId: String, left: String, right: String, output: String): DataFrame = Core.transform(df, operationId, left, right, output)
  def findTrainingData(df: DataFrame, idColumn: String, keys: java.util.List[String]): DataFrame =
    Core.findTrainingData(df, idColumn, keys.asScala.toSeq)
  def label(df: DataFrame, threshold: Double): DataFrame = Core.label(df, threshold)
  def updateLabel(pairs: DataFrame, labels: DataFrame): DataFrame = Core.updateLabel(pairs, labels)
  def inspectTrainingEvidence(df: DataFrame): Array[Long] = {
    val evidence = Core.inspectTrainingEvidence(df)
    Array(evidence.positivePairs, evidence.negativePairs)
  }
  def persist(df: DataFrame, outputPath: String): DataFrame = Core.persist(df, outputPath)
}
