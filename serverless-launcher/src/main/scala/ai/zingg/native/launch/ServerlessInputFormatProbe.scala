package ai.zingg.native.launch

import java.util.ArrayList
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** Exercise the ordinary patched Zingg SparkDFReader over supported file formats. */
object ServerlessInputFormatProbe {
  def run(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType, false),
      StructField("value", DataTypes.StringType, false)))
    val rows = new ArrayList[Row]()
    Seq((1, "alpha"), (2, "beta"), (3, "gamma")).foreach { case (id, value) =>
      rows.add(RowFactory.create(Int.box(id), value))
    }
    val source = spark.createDataFrame(rows, schema)
    val root = sys.props.getOrElse("zingg.native.materialization.runRoot",
      "/Volumes/sda_dev/default/zingg_native_e2e_volume/input-format-probe")
    val paths = Map("parquet" -> s"$root/parquet", "json" -> s"$root/json", "csv" -> s"$root/csv")
    source.write.mode("overwrite").parquet(paths("parquet"))
    source.write.mode("overwrite").json(paths("json"))
    source.write.mode("overwrite").option("header", "true").csv(paths("csv"))
    paths.foreach { case (format, path) =>
      // The patched Zingg assembly is a separate Serverless dependency and
      // must not become a launcher compile-time dependency. Reflect only the
      // ordinary SparkDFReader/Pipe boundary for this integration probe.
      val pipeClass = Class.forName("zingg.common.client.pipe.Pipe")
      val pipe = pipeClass.getDeclaredConstructor().newInstance()
      pipeClass.getMethod("setFormat", classOf[String]).invoke(pipe, format)
      pipeClass.getMethod("setProp", classOf[String], classOf[String]).invoke(pipe, "path", path)
      if (format == "csv") pipeClass.getMethod("setProp", classOf[String], classOf[String]).invoke(pipe, "header", "true")
      val readerClass = Class.forName("zingg.spark.client.util.SparkDFReader")
      val constructor = readerClass.getConstructors.find(_.getParameterCount == 2).getOrElse(
        throw new IllegalStateException("Pinned SparkDFReader(SparkSession, Pipe) constructor is missing"))
      val reader = constructor.newInstance(spark, pipe)
      val loaded = readerClass.getMethod("load").invoke(reader)
      val frame = loaded.getClass.getMethod("df").invoke(loaded).asInstanceOf[Dataset[Row]]
      val count = frame.count()
      require(count == 3L, s"$format reader returned $count rows")
      println(s"NATIVE_INPUT_FORMAT_PASS format=$format rows=$count")
    }
    println("NATIVE_INPUT_FORMAT_SUMMARY formats=parquet,json,csv rows=3")
  }
}
