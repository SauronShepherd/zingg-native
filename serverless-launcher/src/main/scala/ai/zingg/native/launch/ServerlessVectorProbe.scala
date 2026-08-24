package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Direct dense/sparse/null coverage for the public VectorUDT struct extractor. */
object ServerlessVectorProbe {
  def run(spark: SparkSession): Unit = {
    val dense = struct(
      lit(1).alias("type"), lit(null).cast("int").alias("size"),
      lit(null).cast("array<int>").alias("indices"),
      array(lit(0.25d), lit(1.5d), lit(3.5d)).alias("values"))
    val sparse = struct(
      lit(0).alias("type"), lit(4).alias("size"),
      array(lit(1), lit(2)).alias("indices"),
      array(lit(0.2d), lit(0.7d)).alias("values"))
    val input = spark.range(0L, 3L).withColumn(
      "vector", when(col("id") === lit(0L), dense).when(col("id") === lit(1L), sparse))
    val provider = NativeOperationProvider.fromSpark(spark, "model.vector-differential")
    val actual = provider.vectorValue(input, "vector", "value").select("id", "value").orderBy("id").collect()
    require(actual.length == 3, s"Vector probe returned ${actual.length} rows")
    require(actual(0).getDouble(1) == 3.5d, s"Dense vector mismatch: ${actual(0)}")
    require(actual(1).getDouble(1) == 0.7d, s"Sparse vector mismatch: ${actual(1)}")
    require(actual(2).isNullAt(1), s"Null vector mismatch: ${actual(2)}")
    println("NATIVE_VECTOR_DIFFERENTIAL_PASS dense=3.5 sparse=0.7 null=true rows=3")
  }
}
