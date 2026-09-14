package ai.zingg.native

import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class NumericHashCompatibilityTest {
  private val DoubleValues =
    Seq(-12.3456d, -1.999d, -0.049d, 0.0d, 0.049d, 1.999d, 12.3456d)

  private def sparkSession(): SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("NumericHashCompatibilityTest")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  private def frame(
      spark: SparkSession,
      values: Seq[AnyRef],
      dataType: DataType
  ) = {
    val rows = new ArrayList[Row]()
    values.foreach(value => rows.add(RowFactory.create(value)))
    spark.createDataFrame(
      rows,
      StructType(Seq(StructField("value", dataType, true)))
    )
  }

  @Test def truncationMatchesPinnedTowardZeroVectorsAndBoundProperty(): Unit = {
    val spark = sparkSession()
    try {
      val provider =
        NativeOperationProvider.fromSpark(spark, "numeric-hash-compatibility")
      val doubleInput = frame(
        spark,
        DoubleValues.map(java.lang.Double.valueOf),
        DataTypes.DoubleType
      )
      val floatValues = DoubleValues.map(_.toFloat)
      val floatInput = frame(
        spark,
        floatValues.map(java.lang.Float.valueOf),
        DataTypes.FloatType
      )

      val expected = Map(
        1 -> Seq(-12.3d, -1.9d, 0.0d, 0.0d, 0.0d, 1.9d, 12.3d),
        2 -> Seq(-12.34d, -1.99d, -0.04d, 0.0d, 0.04d, 1.99d, 12.34d),
        3 -> Seq(-12.345d, -1.999d, -0.049d, 0.0d, 0.049d, 1.999d, 12.345d)
      )

      (1 to 3).foreach { places =>
        val doubles = provider
          .hash(
            doubleInput,
            s"truncateDoubleTo${places}Places",
            "value",
            "actual"
          )
          .select("actual")
          .collect()
          .map(_.getDouble(0))
          .toSeq
        expected(places).zip(doubles).foreach { case (oracle, actual) =>
          assertEquals(oracle, actual, 1.0e-12d)
        }
        DoubleValues.zip(doubles).foreach { case (source, actual) =>
          assertTrue(math.abs(actual) <= math.abs(source) + 1.0e-12d)
          assertTrue(math.abs(source - actual) < math.pow(10.0d, -places))
          assertTrue(actual == 0.0d || math.signum(actual) == math.signum(source))
        }

        val floats = provider
          .hash(
            floatInput,
            s"truncateFloatTo${places}Places",
            "value",
            "actual"
          )
          .select("actual")
          .collect()
          .map(_.getFloat(0))
          .toSeq
        expected(places).map(_.toFloat).zip(floats).foreach {
          case (oracle, actual) => assertEquals(oracle, actual, 1.0e-6f)
        }
        floatValues.zip(floats).foreach { case (source, actual) =>
          assertTrue(math.abs(actual) <= math.abs(source) + 1.0e-6f)
          assertTrue(
            math.abs(source - actual) < math.pow(10.0d, -places) + 1.0e-6d
          )
          assertTrue(actual == 0.0f || math.signum(actual) == math.signum(source))
        }
      }
    } finally spark.stop()
  }

  @Test def roundMatchesPinnedJavaMathRoundVectorsAndHalfUnitProperty(): Unit = {
    val spark = sparkSession()
    try {
      val provider =
        NativeOperationProvider.fromSpark(spark, "numeric-round-compatibility")
      val values =
        Seq(Double.NaN, -1.6d, -1.5d, -0.5d, 0.49d, 0.5d, 1.5d, 1.6d)
      val input = frame(
        spark,
        values.map(java.lang.Double.valueOf),
        DataTypes.DoubleType
      )
      val actual = provider
        .hash(input, "round", "value", "actual")
        .select("actual")
        .collect()
        .map(_.getLong(0))
        .toSeq
      val expected = Seq(0L, -2L, -1L, 0L, 0L, 1L, 2L, 2L)
      assertEquals(expected, actual)
      values.zip(actual).foreach { case (source, rounded) =>
        if (source.isFinite)
          assertTrue(math.abs(source - rounded.toDouble) <= 0.5d)
      }
    } finally spark.stop()
  }
}
