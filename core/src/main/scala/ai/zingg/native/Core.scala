package ai.zingg.native

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, lower, regexp_extract_all, size, transform, array_intersect, array_union, when}

sealed trait NativeMode
object NativeMode { case object SAFE extends NativeMode; case object EXPERIMENTAL extends NativeMode }
final case class RuntimeDescriptor(sparkVersion: String, scalaVersion: String, photon: Option[Boolean] = None)
final case class NativeContext(spark: SparkSession, mode: NativeMode, runtime: RuntimeDescriptor)
final case class OperationMetadata(id: String, semanticStatus: String, nativeStatus: String)

trait NativeSimilarity { def id: String; def apply(left: Column, right: Column, context: NativeContext): Column }

object ExactSimilarity extends NativeSimilarity {
  val id = "EXACT_SIMILARITY"
  def apply(left: Column, right: Column, context: NativeContext): Column =
    when(left.isNull || right.isNull, lit(1.0)).when(left === right, lit(1.0)).otherwise(lit(0.0))
}

object JaccardSimilarity extends NativeSimilarity {
  val id = "JACCARD_SIMILARITY"
  def apply(left: Column, right: Column, context: NativeContext): Column = {
    val empty = left.isNull || right.isNull || left === lit("") || right === lit("")
    val l = regexp_extract_all(lower(left.cast("string")), lit("[a-z0-9]+"), lit(0))
    val r = regexp_extract_all(lower(right.cast("string")), lit("[a-z0-9]+"), lit(0))
    when(empty, lit(1.0)).otherwise(size(array_intersect(l, r)).cast("double") / size(array_union(l, r)))
  }
}

object SimilarityRegistry {
  private val all = Map[String, NativeSimilarity](ExactSimilarity.id -> ExactSimilarity, JaccardSimilarity.id -> JaccardSimilarity)
  def metadata: Seq[OperationMetadata] = Seq(OperationMetadata(ExactSimilarity.id, "certified", "standard-expression"), OperationMetadata(JaccardSimilarity.id, "certified", "standard-expression"))
  def resolve(id: String, mode: NativeMode): NativeSimilarity = all.getOrElse(id, throw new IllegalArgumentException(s"Unknown or unavailable operation: $id"))
}

object Core {
  val libraryVersion = "0.2.0-SNAPSHOT"
  val protocolVersion = "1"
  def transform(df: DataFrame, operationId: String, left: String, right: String, output: String): DataFrame = {
    val ctx = NativeContext(df.sparkSession, NativeMode.SAFE, RuntimeDescriptor(df.sparkSession.version, util.Properties.versionNumber))
    df.withColumn(output, SimilarityRegistry.resolve(operationId, NativeMode.SAFE)(col(left), col(right), ctx))
  }
}
