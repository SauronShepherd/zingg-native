package ai.zingg.native

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class TruncateParityTest {
  @Test def truncatesFloatingValuesTowardZero(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("TruncateParityTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      val values = Seq[java.lang.Double](12.59d, -12.59d, null).toDF("value")
      val actual = values
        .select(NativeExpressions.truncate(col("value"), 1, "double").alias("actual"))
        .collect()
        .map(row => if (row.isNullAt(0)) null else java.lang.Double.valueOf(row.getDouble(0)))

      assertEquals(12.5d, actual(0).doubleValue(), 1e-12)
      assertEquals(-12.5d, actual(1).doubleValue(), 1e-12)
      assertNull(actual(2))
    } finally spark.stop()
  }
}
