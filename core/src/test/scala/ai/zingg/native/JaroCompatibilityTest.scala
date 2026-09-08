package ai.zingg.native

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class JaroCompatibilityTest {
  @Test def matchesPinnedSecondStringOracleVectorsThroughProductionRegistry(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("JaroCompatibilityTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      // Zingg 0.7.0's SJaroWinkler extends SecondString Jaro directly.  These
      // values therefore lock its historical matching-window semantics rather
      // than a generic textbook Jaro implementation.
      val oracle = Seq(
        ("MARTHA", "MARHTA", 0.9444444444444445d),
        ("DWAYNE", "DUANE", 0.8222222222222223d),
        ("CRATE", "TRACE", 0.9333333333333332d),
        ("abc", "xyz", 0.0d),
        ("Same", "same", 1.0d),
        ("", "abc", 1.0d))
      val vectors = oracle
        .flatMap { case (left, right, expected) =>
          Seq(
            (left, right, expected),
            (right, left, expected))
        }
        .toDF("left", "right", "expected")

      val rows = Core
        .transform(vectors, "JARO_SIMILARITY", "left", "right", "actual")
        .select(col("actual"), col("expected"))
        .as[(Double, Double)]
        .collect()

      assertEquals(oracle.size * 2, rows.length)
      rows.foreach { case (actual, expected) =>
        assertEquals(expected, actual, 1e-12)
      }
    } finally spark.stop()
  }
}
