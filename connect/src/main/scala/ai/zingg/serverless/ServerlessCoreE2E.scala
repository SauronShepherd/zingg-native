package ai.zingg.serverless

import ai.zingg.native.Core
import org.apache.spark.sql.SparkSession

/** Non-interactive Serverless JAR-task probe for the shared Scala core. */
object ServerlessCoreE2E {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      import spark.implicits._
      val input = Seq(("a", "New York", "new-york"), ("b", "x", "y"))
        .toDF("record_id", "left_value", "right_value")
      val scored = Core.transform(input, "JACCARD_SIMILARITY", "left_value", "right_value", "z_jaccard")
      val rows = scored.select("record_id", "z_jaccard").orderBy("record_id").collect()
      require(rows.length == 2, s"expected two rows, got ${rows.length}")
      require(rows(0).getString(0) == "a" && math.abs(rows(0).getDouble(1) - 1.0) < 1e-12,
        s"unexpected first result: ${rows(0)}")
      require(rows(1).getString(0) == "b" && math.abs(rows(1).getDouble(1)) < 1e-12,
        s"unexpected second result: ${rows(1)}")
      println(s"ZINGG_NATIVE_SERVERLESS_CORE_E2E PASS rows=${rows.length} spark=${spark.version}")
    } finally {
      spark.stop()
    }
  }
}
