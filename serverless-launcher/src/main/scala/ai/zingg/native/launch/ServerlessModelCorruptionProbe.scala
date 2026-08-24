package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Expected-failure probe for fail-closed native model persistence. */
object ServerlessModelCorruptionProbe {
  def run(spark: SparkSession): Unit = {
    val path = sys.props.getOrElse(
      "zingg.native.model.corruption.probe.path",
      "/Volumes/sda_dev/default/zingg_native_e2e_volume/model-corruption-probe")
    val sidecar = s"${path.stripSuffix("/")}/_zingg_native_model_v1"
    spark.range(1L).select(lit(1).alias("schemaVersion")).write.mode("overwrite").parquet(sidecar)
    val provider = NativeOperationProvider.fromSpark(spark, "model.persistence.corruption.probe")
    try {
      provider.loadModel(path)
      throw new IllegalStateException("Corrupt native model sidecar was accepted")
    } catch {
      case e: Exception if Option(e.getMessage).exists(_.contains("model.nativePersistence.load")) =>
        println("NATIVE_MODEL_CORRUPTION_EXPECTED_FAILURE rule=model.nativePersistence.load")
      case e: Exception =>
        throw new IllegalStateException(s"Corrupt model failure lacked persistence rule identifier: ${e.getMessage}", e)
    }
  }
}
