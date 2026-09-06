package ai.zingg.native

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class JaroCompatibilityTest {
  @Test def matchesPinnedSecondStringOracleVectors(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("JaroCompatibilityTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      val vectors = Seq(
        ("MARTHA", "MARHTA", 0.9444444444444445d),
        ("DWAYNE", "DUANE", 0.8222222222222223d),
        ("CRATE", "TRACE", 0.8666666666666667d),
        ("abc", "xyz", 0.0d),
        ("Same", "same", 1.0d),
        ("", "abc", 0.0d))
        .toDF("left", "right", "expected")

      val rows = vectors
        .select(PublicRewriteRules.jaro(col("left"), col("right")).alias("actual"), col("expected"))
        .as[(Double, Double)]
        .collect()

      rows.foreach { case (actual, expected) =>
        assertEquals(expected, actual, 1e-12)
      }
    } finally spark.stop()
  }
}
