package ai.zingg.native

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.util.UUID

/** GraphFrames-free connected components built only with relational operators. */
object NativeGraph {
  def connectedComponents(
      vertices:DataFrame,
      edges:DataFrame,
      idColumn:String,
      rightIdColumn:String,
      clusterColumn:String,
      context:RewriteContext,
      maxIterations:Int=128):DataFrame={
    require(maxIterations>0,"maxIterations must be positive")
    val effectiveMaxIterations = sys.props.get("zingg.native.graph.maxIterations")
      .map(_.trim).filter(_.nonEmpty).map(_.toInt).map { overrideValue =>
        require(overrideValue > 0, "zingg.native.graph.maxIterations must be positive")
        math.min(maxIterations, overrideValue)
      }.getOrElse(maxIterations)
    val original=vertices
    val ids=original.select(col(idColumn).alias("_id")).distinct()
    val directed=edges.select(col(idColumn).alias("src"),col(rightIdColumn).alias("dst"))
      .unionByName(edges.select(col(rightIdColumn).alias("src"),col(idColumn).alias("dst"))).filter(col("src").isNotNull&&col("dst").isNotNull).distinct()
    val materializationRoot=sys.props.getOrElse(
      "zingg.native.graph.materializePath", "dbfs:/tmp/zingg-native-graph")
    val materializationPath=s"$materializationRoot/${UUID.randomUUID().toString}"
    // Cut the upstream prediction lineage once before fixed-point iteration.
    // Without this boundary, every convergence action re-plans the complete
    // match/link feature and prediction graph through Spark Connect.
    val stableIdsPath=s"$materializationPath/ids"
    val stableDirectedPath=s"$materializationPath/directed"
    ids.write.mode("overwrite").parquet(stableIdsPath)
    directed.write.mode("overwrite").parquet(stableDirectedPath)
    val stableIds=vertices.sparkSession.read.parquet(stableIdsPath)
    val stableDirected=vertices.sparkSession.read.parquet(stableDirectedPath)
    var labels=stableIds.withColumn("_component",col("_id"))
    var i=0
    var done=false
    // Materialize each fixed-point step through the public DataFrame writer.
    // This keeps the Spark Connect plan bounded instead of nesting one full
    // join lineage inside the next iteration.
    while(i<effectiveMaxIterations && !done){
      val propagated=stableDirected.alias("e").join(labels.alias("l"),col("e.src")===col("l._id"),"inner")
        .select(col("e.dst").alias("_id"),col("l._component"))
      val next=labels.select("_id","_component").unionByName(propagated)
        .groupBy("_id").agg(min(col("_component")).alias("_component"))
      val stepPath=s"$materializationPath/labels-$i"
      // Persist the computed step before convergence inspection. The old
      // order executed the complete join/groupBy once for convergence and a
      // second time for the write, which is especially costly on Serverless.
      next.write.mode("overwrite").parquet(stepPath)
      val materializedNext=vertices.sparkSession.read.parquet(stepPath)
      done=materializedNext.alias("n").join(labels.alias("o"),col("n._id")===col("o._id"),"inner")
        .filter(!(col("n._component") <=> col("o._component"))).limit(1).count()==0
      labels=materializedNext
      i+=1
    }
    if(!done && context.mode==NativeExecutionMode.STRICT)
      throw new NativeRewriteUnsupportedException(s"Connected-components did not converge within $effectiveMaxIterations iterations")
    NativeEvidenceCollector.recordRule(context,"rewrite.graph.connected_components")
    NativePlanGuard.guardDataFrame(
      original.join(labels,original(idColumn)===labels("_id"),"left").drop("_id").withColumnRenamed("_component",clusterColumn), context)
  }
}
