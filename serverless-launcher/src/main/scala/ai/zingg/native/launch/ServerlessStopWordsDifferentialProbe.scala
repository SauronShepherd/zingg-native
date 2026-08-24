package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** Stop-word preprocessing parity against the pinned Zingg UDF oracle. */
object ServerlessStopWordsDifferentialProbe {
  def run(spark: SparkSession): Unit = {
    val values = Seq[String](null, "", "the quick brown fox", "The and THE", "sand theatre", "one, and one")
    val schema = StructType(Seq(StructField("value", DataTypes.StringType, nullable = true)))
    val rows = new ArrayList[Row](); values.foreach(v => rows.add(RowFactory.create(v)))
    val input = spark.createDataFrame(rows, schema)
    val pattern = "(?i)\\b(?:the|and)\\b"
    val provider = NativeOperationProvider.fromSpark(spark, "preprocess-stopwords-differential")
    val actual = provider.removeStopWords(input, "value", pattern).select("value").collect()
      .map(row => if (row.isNullAt(0)) null else row.getString(0))
    val oracleClass = Class.forName("zingg.spark.core.preprocess.stopwords.RemoveStopWordsUDF")
    val oracle = oracleClass.getDeclaredConstructor().newInstance()
    val call = oracleClass.getMethod("call", classOf[String], classOf[String])
    values.zip(actual).zipWithIndex.foreach { case ((value, nativeValue), index) =>
      val expected = call.invoke(oracle, value, pattern).asInstanceOf[String]
      require(expected == nativeValue,
        s"Stop-word differential mismatch row=$index native=$nativeValue oracle=$expected")
    }
    println(s"NATIVE_PREPROCESS_DIFFERENTIAL_PASS rule=stopWords rows=${values.size}")
  }
}
