package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/** Numeric similarity parity against the pinned Zingg 0.7 classes. */
object ServerlessNumericDifferentialProbe {
  private final case class Rule(operation: String, values: Seq[(AnyRef, AnyRef)], sparkType: DataType)

  private val Rules = Seq(
    Rule("IntegerSimilarityFunction", Seq(
      (null, null), (Integer.valueOf(Int.MinValue), Integer.valueOf(Int.MinValue)),
      (Integer.valueOf(-1), Integer.valueOf(1)), (Integer.valueOf(0), Integer.valueOf(1)),
      (Integer.valueOf(Int.MaxValue), Integer.valueOf(Int.MaxValue - 1))), DataTypes.IntegerType),
    Rule("LongSimilarityFunction", Seq(
      (null, null), (java.lang.Long.valueOf(Long.MinValue), java.lang.Long.valueOf(Long.MinValue)),
      (java.lang.Long.valueOf(-1L), java.lang.Long.valueOf(1L)),
      (java.lang.Long.valueOf(Long.MaxValue), java.lang.Long.valueOf(Long.MaxValue - 1L))), DataTypes.LongType),
    Rule("DoubleSimilarityFunction", Seq(
      (null, null), (java.lang.Double.valueOf(-1.0d), java.lang.Double.valueOf(1.0d)),
      (java.lang.Double.valueOf(0.0d), java.lang.Double.valueOf(0.0d)),
      (java.lang.Double.valueOf(Double.NaN), java.lang.Double.valueOf(Double.NaN)),
      (java.lang.Double.valueOf(Double.PositiveInfinity), java.lang.Double.valueOf(Double.PositiveInfinity))), DataTypes.DoubleType),
    Rule("FloatSimilarityFunction", Seq(
      (null, null), (java.lang.Float.valueOf(-1.0f), java.lang.Float.valueOf(1.0f)),
      (java.lang.Float.valueOf(0.0f), java.lang.Float.valueOf(0.0f)),
      (java.lang.Float.valueOf(Float.NaN), java.lang.Float.valueOf(Float.NaN))), DataTypes.FloatType))

  def run(spark: SparkSession): Unit = {
    val provider = NativeOperationProvider.fromSpark(spark, "numeric-differential")
    Rules.foreach { rule =>
      val schema = StructType(Seq(
        StructField("left_value", rule.sparkType, nullable = true),
        StructField("right_value", rule.sparkType, nullable = true)))
      val rows = new ArrayList[Row]()
      rule.values.foreach { case (left, right) => rows.add(RowFactory.create(left, right)) }
      val input = spark.createDataFrame(rows, schema)
      val actual = provider.similarityByZinggName(input, rule.operation, "left_value", "right_value", "actual")
        .select("actual").collect().map(_.getAs[java.lang.Double](0).doubleValue())
      val oracleClass = Class.forName(s"zingg.common.core.similarity.function.${rule.operation}")
      val oracle = oracleClass.getDeclaredConstructor().newInstance()
      val call = oracleClass.getMethods.find(m => m.getName == "call" && m.getParameterCount == 2).get
      val expected = rule.values.map { case (left, right) =>
        call.invoke(oracle, left, right).asInstanceOf[java.lang.Double].doubleValue()
      }
      actual.zip(expected).zipWithIndex.foreach { case ((nativeValue, referenceValue), index) =>
        require((nativeValue.isNaN && referenceValue.isNaN) || math.abs(nativeValue - referenceValue) <= 1e-9,
          s"Numeric differential mismatch rule=${rule.operation} row=$index native=$nativeValue oracle=$referenceValue")
      }
      println(s"NATIVE_NUMERIC_DIFFERENTIAL_PASS rule=${rule.operation} rows=${actual.length}")
    }
    println(s"NATIVE_NUMERIC_DIFFERENTIAL_SUMMARY rules=${Rules.size} rows=${Rules.map(_.values.size).sum}")
  }
}
