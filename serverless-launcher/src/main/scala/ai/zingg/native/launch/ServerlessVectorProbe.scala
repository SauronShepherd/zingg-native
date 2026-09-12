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
    val sparsePresent = struct(
      lit(0).alias("type"), lit(4).alias("size"),
      array(lit(1), lit(2)).alias("indices"),
      array(lit(0.2d), lit(0.7d)).alias("values"))
    val sparseMissing = struct(
      lit(0).alias("type"), lit(4).alias("size"),
      array(lit(2), lit(3)).alias("indices"),
      array(lit(0.7d), lit(0.9d)).alias("values"))
    val input = spark.range(0L, 4L).withColumn(
      "vector",
      when(col("id") === lit(0L), dense)
        .when(col("id") === lit(1L), sparsePresent)
        .when(col("id") === lit(2L), sparseMissing))
    val provider = NativeOperationProvider.fromSpark(spark, "model.vector-differential")
    val actual = provider.vectorValue(input, "vector", "value").select("id", "value").orderBy("id").collect()
    require(actual.length == 4, s"Vector probe returned ${actual.length} rows")
    require(actual(0).getDouble(1) == 1.5d, s"Dense vector mismatch: ${actual(0)}")
    require(actual(1).getDouble(1) == 0.2d, s"Sparse-present vector mismatch: ${actual(1)}")
    require(actual(2).getDouble(1) == 0.0d, s"Sparse-missing vector mismatch: ${actual(2)}")
    require(actual(3).isNullAt(1), s"Null vector mismatch: ${actual(3)}")
    println("NATIVE_VECTOR_DIFFERENTIAL_PASS dense=1.5 sparsePresent=0.2 sparseMissing=0.0 null=true rows=4")
  }
}
