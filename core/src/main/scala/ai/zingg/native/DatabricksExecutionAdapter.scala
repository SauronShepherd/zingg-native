package ai.zingg.native

import org.apache.spark.sql.{DataFrame,SparkSession}

/**
 * Tiny transport-neutral runtime adapter. The rewrite registry is shared; this
 * class only describes how the application reached Spark and captures evidence.
 */
sealed trait SparkTransport { def id:String }
object SparkTransport { case object ClassicPy4J extends SparkTransport{val id="classic-py4j"}; case object SparkConnect extends SparkTransport{val id="spark-connect"} }

final case class DatabricksExecutionAdapter(spark:SparkSession,transport:SparkTransport,mode:NativeExecutionMode,runId:String){
  def context(phase:String):RewriteContext=RewriteContext(spark,mode,RuntimeDescriptor(spark.version,"2.13"),phase,runId,
    Map("transport"->transport.id,"disabledRules"->sys.env.getOrElse("ZINGG_NATIVE_DISABLED_RULES","")))
  def inspect(df:DataFrame,phase:String,photonEvidence:Option[String]=None):NativeExecutionEvidence=NativeEvidenceCollector.capture(df,context(phase),photonEvidence)
}
