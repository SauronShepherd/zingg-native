package ai.zingg.native.gateway

import ai.zingg.native.{Core, NativeRewriteRegistry}
import org.apache.spark.sql.DataFrame
import scala.jdk.CollectionConverters._

/**
 * Optional Py4J transport for Dedicated/Classic clients.  It exposes the same
 * rewrite registry as the Zingg integration provider and no Zingg phase logic.
 */
class ClassicGateway {
  def libraryVersion: String = Core.libraryVersion
  def protocolVersion: String = Core.protocolVersion
  def capabilityMetadata: String =
    "native-rewrite-registry;public-spark-api;classic-py4j-transport;photon-proof-requires-runtime-evidence"
  def sparkVersion(df: DataFrame): String = df.sparkSession.version
  def supportedOperations: Array[String] = NativeRewriteRegistry.default.operationIds.toArray
  def supportedPhases: Array[String] = Array.empty[String]

  def preprocess(df: DataFrame, operationId: String, columns: java.util.List[String]): DataFrame =
    Core.preprocess(df, operationId, columns.asScala.toSeq)

  def transform(
      df: DataFrame,
      operationId: String,
      left: String,
      right: String,
      output: String): DataFrame =
    Core.transform(df, operationId, left, right, output)
}
