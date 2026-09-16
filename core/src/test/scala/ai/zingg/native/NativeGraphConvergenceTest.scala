package ai.zingg.native

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class NativeGraphConvergenceTest {
  @Test def equalAggregateLabelsDoNotImplyConvergence(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("NativeGraphConvergenceTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      val left = Seq((1L, 1L, 1L), (2L, 4L, 1L)).toDF("src", "min_nbr", "cnt")
      val collision = Seq((1L, 2L, 1L), (2L, 3L, 1L)).toDF("src", "min_nbr", "cnt")
      val same = Seq((2L, 4L, 9L), (1L, 1L, 7L)).toDF("src", "min_nbr", "cnt")

      assertTrue(left.selectExpr("sum(min_nbr)").head().getLong(0) == collision.selectExpr("sum(min_nbr)").head().getLong(0))
      assertFalse(NativeGraph.sameAssignments(left, collision))
      assertTrue(NativeGraph.sameAssignments(left, same))
    } finally {
      spark.stop()
    }
  }
}
