package ai.zingg.native

import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class RoundHashCompatibilityTest {
  @Test def matchesPinnedSparkRoundVectorsAndRoundingProperties(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("RoundHashCompatibilityTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      val values = Seq[java.lang.Double](
        null,
        java.lang.Double.valueOf(Double.NaN),
        java.lang.Double.valueOf(Double.NegativeInfinity),
        java.lang.Double.valueOf(-1.5d),
        java.lang.Double.valueOf(-0.5d),
        java.lang.Double.valueOf(0.0d),
        java.lang.Double.valueOf(java.lang.Math.nextDown(0.5d)),
        java.lang.Double.valueOf(0.5d),
        java.lang.Double.valueOf(1.5d),
        java.lang.Double.valueOf(Double.PositiveInfinity)
      )
      val expected = Seq[java.lang.Long](
        null,
        java.lang.Long.valueOf(0L),
        java.lang.Long.valueOf(Long.MinValue),
        java.lang.Long.valueOf(-1L),
        java.lang.Long.valueOf(0L),
        java.lang.Long.valueOf(0L),
        java.lang.Long.valueOf(0L),
        java.lang.Long.valueOf(1L),
        java.lang.Long.valueOf(2L),
        java.lang.Long.valueOf(Long.MaxValue)
      )

      val rows = new ArrayList[Row]()
      values.foreach(value => rows.add(RowFactory.create(value)))
      val input = spark.createDataFrame(
        rows,
        StructType(Seq(StructField("value", DataTypes.DoubleType, true)))
      )
      val provider =
        NativeOperationProvider.fromSpark(spark, "round-hash-compatibility")
      val actual = provider
        .hash(input, "round", "value", "actual")
        .select("actual")
        .collect()
        .map(row =>
          if (row.isNullAt(0)) null else java.lang.Long.valueOf(row.getLong(0))
        )
        .toSeq

      // These values pin Zingg 0.7.0 SparkRound's java.lang.Math.round contract.
      // nextDown(0.5) is the historical edge where floor(x + 0.5) is wrong.
      assertEquals(expected, actual)

      val finite = values.zip(actual).collect {
        case (source, result)
            if source != null && result != null && java.lang.Double.isFinite(
              source.doubleValue()
            ) =>
          source.doubleValue() -> result.longValue()
      }
      finite.foreach { case (source, result) =>
        assertTrue(
          math.abs(result.toDouble - source) <= 0.5d,
          s"rounding error exceeds half a unit source=$source result=$result"
        )
      }
      val ordered = finite.sortBy(_._1).map(_._2)
      assertEquals(ordered.sorted, ordered)
    } finally spark.stop()
  }
}
