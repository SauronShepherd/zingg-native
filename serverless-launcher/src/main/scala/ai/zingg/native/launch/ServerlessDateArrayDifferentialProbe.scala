package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import java.sql.Date
import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

/** Date and array similarity parity against the pinned Zingg 0.7 classes. */
object ServerlessDateArrayDifferentialProbe {
  private def same(left: Double, right: Double): Boolean =
    (left.isNaN && right.isNaN) || math.abs(left - right) <= 1e-9

  def run(spark: SparkSession): Unit = {
    val provider = NativeOperationProvider.fromSpark(spark, "date-array-differential")
    val dateSchema = StructType(Seq(
      StructField("left_value", DataTypes.DateType, nullable = true),
      StructField("right_value", DataTypes.DateType, nullable = true)))
    val dates = Seq[(Date, Date)](
      (null, null),
      (Date.valueOf("1970-01-01"), Date.valueOf("1970-01-01")),
      (Date.valueOf("1970-01-01"), Date.valueOf("1970-01-02")),
      (Date.valueOf("2000-02-29"), Date.valueOf("2001-02-28")),
      (Date.valueOf("2024-03-10"), Date.valueOf("2024-03-11")),
      (Date.valueOf("2024-11-03"), Date.valueOf("2024-11-04")),
      (Date.valueOf("1900-01-01"), Date.valueOf("2100-01-01")))
    val dateRows = new ArrayList[Row](); dates.foreach { case (l, r) => dateRows.add(RowFactory.create(l, r)) }
    val dateClass = Class.forName("zingg.common.core.similarity.function.DateSimilarityFunction")
    val dateOracle = dateClass.getDeclaredConstructor().newInstance()
    val dateCall = dateClass.getMethod("call", classOf[java.util.Date], classOf[java.util.Date])
    val expectedDates = dates.map { case (left, right) =>
      dateCall.invoke(dateOracle, left, right).asInstanceOf[java.lang.Double].doubleValue()
    }
    val originalZone = spark.conf.getOption("spark.sql.session.timeZone")
    val zones = Seq("UTC", "America/Los_Angeles", "Asia/Kathmandu")
    try {
      zones.foreach { zone =>
        spark.conf.set("spark.sql.session.timeZone", zone)
        val dateInput = spark.createDataFrame(dateRows, dateSchema)
        val dateActual = provider.similarityByZinggName(dateInput, "DateSimilarityFunction", "left_value", "right_value", "actual")
          .select("actual").collect().map(_.getAs[java.lang.Double](0).doubleValue())
        expectedDates.zip(dateActual).foreach { case (expected, actual) =>
          require(same(actual, expected), s"Date differential mismatch zone=$zone native=$actual oracle=$expected")
        }
      }
    } finally {
      originalZone match {
        case Some(zone) => spark.conf.set("spark.sql.session.timeZone", zone)
        case None => spark.conf.unset("spark.sql.session.timeZone")
      }
    }
    println(s"NATIVE_TYPED_DIFFERENTIAL_PASS rule=DateSimilarityFunction rows=${dates.size} zones=${zones.size}")

    val arraySchema = StructType(Seq(
      StructField("left_value", ArrayType(DataTypes.DoubleType, containsNull = true), nullable = true),
      StructField("right_value", ArrayType(DataTypes.DoubleType, containsNull = true), nullable = true)))
    val arrays = Seq[(Array[java.lang.Double], Array[java.lang.Double])](
      (null, null),
      (Array(java.lang.Double.valueOf(1.0d), java.lang.Double.valueOf(0.0d)), Array(java.lang.Double.valueOf(1.0d), java.lang.Double.valueOf(0.0d))),
      (Array(java.lang.Double.valueOf(1.0d), java.lang.Double.valueOf(2.0d)), Array(java.lang.Double.valueOf(2.0d), java.lang.Double.valueOf(1.0d))),
      (Array(java.lang.Double.valueOf(0.0d), java.lang.Double.valueOf(0.0d)), Array(java.lang.Double.valueOf(1.0d), java.lang.Double.valueOf(1.0d))))
    val arrayRows = new ArrayList[Row](); arrays.foreach { case (l, r) => arrayRows.add(RowFactory.create(l, r)) }
    val arrayInput = spark.createDataFrame(arrayRows, arraySchema)
    val arrayActual = provider.similarityByZinggName(arrayInput, "ArrayDoubleSimilarityFunction", "left_value", "right_value", "actual")
      .select("actual").collect().map(_.getAs[java.lang.Double](0).doubleValue())
    val arrayClass = Class.forName("zingg.common.core.similarity.function.ArrayDoubleSimilarityFunction")
    val arrayOracle = arrayClass.getDeclaredConstructor().newInstance()
    val arrayCall = arrayClass.getMethod("call", classOf[Array[java.lang.Double]], classOf[Array[java.lang.Double]])
    arrays.zip(arrayActual).foreach { case ((left, right), actual) =>
      val expected = arrayCall.invoke(arrayOracle, left, right).asInstanceOf[java.lang.Double].doubleValue()
      require(same(actual, expected), s"Array differential mismatch native=$actual oracle=$expected")
    }
    println(s"NATIVE_TYPED_DIFFERENTIAL_PASS rule=ArrayDoubleSimilarityFunction rows=${arrays.size}")
    println(s"NATIVE_TYPED_DIFFERENTIAL_SUMMARY rules=2 rows=${dates.size + arrays.size}")
  }
}
