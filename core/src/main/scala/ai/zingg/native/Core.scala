package ai.zingg.native

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, array_intersect, array_union, col, concat, element_at, floor, greatest, least, length, lit, lower, regexp_extract_all, sequence, size, struct, transform, when, zip_with, aggregate}

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

/** SecondString Jaro semantics used by Zingg 0.7 (not Jaro-Winkler). */
object JaroSimilarity extends NativeSimilarity {
  val id = "JARO_SIMILARITY"
  def apply(left: Column, right: Column, context: NativeContext): Column = {
    val first = lower(left.cast("string"))
    val second = lower(right.cast("string"))
    val n = length(first)
    val m = length(second)
    val chars1 = org.apache.spark.sql.functions.split(first, "")
    val chars2 = org.apache.spark.sql.functions.split(second, "")
    val half = floor(least(n, m) / lit(2)).cast("int") + lit(1)

    def common(source: Column, target: Column, sourceLen: Column): Column = {
      val indexes = sequence(lit(0), greatest(sourceLen - lit(1), lit(0)))
      val initial = struct(array().cast("array<string>").alias("common"), target.alias("remaining"))
      val result = aggregate(indexes, initial, (state: Column, i: Column) => {
        val start = greatest(lit(0), i - half)
        val stop = least(size(state.getField("remaining")) - lit(1), i + half - lit(1))
        val candidates = sequence(start, greatest(start, stop))
        val found = aggregate(candidates, struct(lit(false).alias("matched"), lit(-1).alias("index")),
          (hit: Column, j: Column) => when(hit.getField("matched"), hit).otherwise(
            when(element_at(source, i + lit(1)) === element_at(state.getField("remaining"), j + lit(1)),
              struct(lit(true).alias("matched"), j.alias("index"))).otherwise(hit)))
        val marked = transform(state.getField("remaining"), (value: Column, j: Column) =>
          when(j === found.getField("index"), lit("*")).otherwise(value))
        struct(
          when(found.getField("matched"), concat(state.getField("common"), array(element_at(source, i + lit(1)))))
            .otherwise(state.getField("common")).alias("common"),
          when(found.getField("matched"), marked).otherwise(state.getField("remaining")).alias("remaining"))
      })
      result.getField("common")
    }
    val common1 = common(chars1, chars2, n)
    val common2 = common(chars2, chars1, m)
    val commonCount = size(common1)
    val mismatches = aggregate(zip_with(common1, common2, (a: Column, b: Column) => (a =!= b).cast("int")), lit(0),
      (total: Column, mismatch: Column) => total + org.apache.spark.sql.functions.coalesce(mismatch, lit(0)))
    val score = ((commonCount / n) + (commonCount / m) + ((commonCount - floor(mismatches / lit(2))) / commonCount)) / lit(3.0)
    when(left.isNull || right.isNull || length(left) === lit(0) || length(right) === lit(0), lit(1.0))
      .otherwise(when(commonCount === lit(0) || n === lit(0) || m === lit(0), lit(0.0)).otherwise(score))
  }
}

object SimilarityRegistry {
  private val all = Map[String, NativeSimilarity](ExactSimilarity.id -> ExactSimilarity, JaccardSimilarity.id -> JaccardSimilarity, JaroSimilarity.id -> JaroSimilarity)
  def metadata: Seq[OperationMetadata] = Seq(OperationMetadata(ExactSimilarity.id, "certified", "standard-expression"), OperationMetadata(JaccardSimilarity.id, "certified", "standard-expression"), OperationMetadata(JaroSimilarity.id, "certified", "standard-expression-fallback"))
  def resolve(id: String, mode: NativeMode): NativeSimilarity = all.getOrElse(id, throw new IllegalArgumentException(s"Unknown or unavailable operation: $id"))
}

object Core {
  val libraryVersion = "0.2.0-SNAPSHOT"
  val protocolVersion = "1"
  def transform(df: DataFrame, operationId: String, left: String, right: String, output: String): DataFrame = {
    val ctx = NativeContext(df.sparkSession, NativeMode.SAFE, RuntimeDescriptor(df.sparkSession.version, "2.13"))
    df.withColumn(output, SimilarityRegistry.resolve(operationId, NativeMode.SAFE)(col(left), col(right), ctx))
  }
}
