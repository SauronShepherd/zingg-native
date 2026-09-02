package ai.zingg.native

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import java.util.UUID

/** GraphFrames-compatible connected components using public relational Spark APIs. */
object NativeGraph {
  private val Src = "src"
  private val Dst = "dst"
  private val MinNbr = "min_nbr"
  private val Count = "cnt"
  private def minValue(x: Column, y: Column): Column = when(x < y, x).otherwise(y)
  private def maxValue(x: Column, y: Column): Column = when(x > y, x).otherwise(y)
  private def symmetrize(e: DataFrame): DataFrame = e.select(explode(array(
    struct(col(Src), col(Dst)), struct(col(Dst).alias(Src), col(Src).alias(Dst)))).alias("edge"))
    .select(col("edge.src").alias(Src), col("edge.dst").alias(Dst))
  private def minNbrs(e: DataFrame): DataFrame = symmetrize(e).groupBy(Src)
    .agg(min(col(Dst)).alias(MinNbr), count(lit(1)).alias(Count))
    .withColumn(MinNbr, minValue(col(Src), col(MinNbr)))
  private def sameAssignments(left: DataFrame, right: DataFrame): Boolean = {
    val leftAssignments = left.select(col(Src), col(MinNbr))
    val rightAssignments = right.select(col(Src), col(MinNbr))
    leftAssignments.except(rightAssignments).isEmpty && rightAssignments.except(leftAssignments).isEmpty
  }

  /** Port of GraphFrames' default two_phase (large-star/small-star) algorithm. */
  def connectedComponents(vertices: DataFrame, edges: DataFrame, idColumn: String,
      rightIdColumn: String, clusterColumn: String, context: RewriteContext,
      maxIterations: Int = Int.MaxValue): DataFrame = {
    require(maxIterations > 0, "maxIterations must be positive")
    require(sys.props.getOrElse("zingg.native.graph.strategy", "two_phase").trim.toLowerCase == "two_phase",
      "zingg.native.graph.strategy must be two_phase")
    val spark = vertices.sparkSession
    val root = s"${sys.props.getOrElse("zingg.native.graph.materializePath", "dbfs:/tmp/zingg-native-graph")}/${UUID.randomUUID()}"
    val ids = vertices.select(col(idColumn).alias("id")).distinct()
    val initial = edges.select(col(idColumn).alias(Src), col(rightIdColumn).alias(Dst))
      .unionByName(edges.select(col(rightIdColumn).alias(Src), col(idColumn).alias(Dst)))
      .filter(col(Src).isNotNull && col(Dst).isNotNull && col(Src) =!= col(Dst))
      .select(minValue(col(Src), col(Dst)).alias(Src), maxValue(col(Src), col(Dst)).alias(Dst)).distinct()
    initial.write.mode("overwrite").parquet(s"$root/edges-0")
    var ee = spark.read.parquet(s"$root/edges-0")
    var nbrs = minNbrs(ee)
    var iteration = 1
    var converged = false
    while (!converged && iteration <= maxIterations) {
      val large = ee.join(nbrs, Seq(Src)).select(col(Dst).alias(Src), col(MinNbr).alias(Dst)).distinct()
      val smallNbrs = large.groupBy(Src).agg(min(col(Dst)).alias(MinNbr), count(lit(1)).alias(Count))
      val next = large.join(smallNbrs, Seq(Src)).select(col(MinNbr).alias(Src), col(Dst))
        .filter(col(Src) =!= col(Dst))
        .unionByName(smallNbrs.select(col(MinNbr).alias(Src), col(Src).alias(Dst))).distinct()
      next.write.mode("overwrite").parquet(s"$root/edges-$iteration")
      ee = spark.read.parquet(s"$root/edges-$iteration")
      val nextNbrs = minNbrs(ee)
      converged = sameAssignments(nbrs, nextNbrs)
      NativeDiagnostics.graphIteration(context, iteration, converged)
      nbrs = nextNbrs
      iteration += 1
    }
    if (!converged)
      throw new NativeRewriteUnsupportedException(s"Connected-components did not converge within $maxIterations iterations")
    val labels = ids.join(nbrs, ids("id") === nbrs(Src), "left")
      .select(ids("id"), coalesce(nbrs(MinNbr), ids("id")).alias("_component"))
    NativeEvidenceCollector.recordRule(context, "rewrite.graph.connected_components")
    NativePlanGuard.guardDataFrame(vertices.join(labels, vertices(idColumn) === labels("id"), "left")
      .drop("id").withColumnRenamed("_component", clusterColumn), context)
  }
}
