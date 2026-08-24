package ai.zingg.native.launch

import ai.zingg.native.NativeMaterializationLifecycle
import org.apache.spark.sql.SparkSession

/** Two-job probe: the first leaves a sentinel then fails; the second proves cleanup enabled recovery. */
object ServerlessMaterializationRecoveryProbe {
  def leaveFailedSentinel(spark: SparkSession): Unit = {
    val root = sys.props.getOrElse("zingg.native.materialization.runRoot",
      throw new IllegalStateException("missing native transient run root"))
    spark.range(1L).write.mode("overwrite").parquet(s"$root/failure-sentinel")
    println(s"NATIVE_MATERIALIZATION_FAILURE_SENTINEL_WRITTEN root=$root")
    throw new RuntimeException("intentional materialization recovery probe failure")
  }

  def verifyRecovered(spark: SparkSession): Unit = {
    val root = sys.props.getOrElse("zingg.native.materialization.runRoot",
      throw new IllegalStateException("missing native transient run root"))
    require(!NativeMaterializationLifecycle.exists(root),
      s"native transient run root survived failed-task cleanup: $root")
    println(s"NATIVE_MATERIALIZATION_RECOVERY_PASS root=$root exists=false")
  }
}
