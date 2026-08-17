package ai.zingg.serverless

import ai.zingg.native.Core
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/** Non-interactive Serverless JAR-task probe for the shared Scala core. */
object ServerlessCoreE2E {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      import spark.implicits._
      val input = Seq(("a", "New York", "new-york"), ("b", "x", "y"))
        .toDF("record_id", "left_value", "right_value")
      val scored = Core.transform(input, "JACCARD_SIMILARITY", "left_value", "right_value", "z_jaccard")
      val rows = scored.select("record_id", "z_jaccard").orderBy("record_id").collect()
      require(rows.length == 2, s"expected two rows, got ${rows.length}")
      require(rows(0).getString(0) == "a" && math.abs(rows(0).getDouble(1) - 1.0) < 1e-12,
        s"unexpected first result: ${rows(0)}")
      require(rows(1).getString(0) == "b" && math.abs(rows(1).getDouble(1)) < 1e-12,
        s"unexpected second result: ${rows(1)}")
      val exactRows = Core.transform(input, "EXACT_SIMILARITY", "left_value", "right_value", "z_exact")
        .select("record_id", "z_exact").orderBy("record_id").collect()
      require(exactRows(0).getDouble(1) == 0.0 && exactRows(1).getDouble(1) == 0.0,
        s"unexpected exact results: ${exactRows.mkString(",")}")
      val jaroInput = Seq(("m", "MARTHA", "MARHTA")).toDF("record_id", "left_value", "right_value")
      val jaroRows = Core.transform(jaroInput, "JARO_SIMILARITY", "left_value", "right_value", "z_jaro")
        .select("z_jaro").collect()
      require(math.abs(jaroRows(0).getDouble(0) - 0.9444444444444445) < 1e-12,
        s"unexpected jaro result: ${jaroRows(0)}")

      val records = Seq(
        ("a", "Alice", "Madrid"),
        ("b", "alice", "madrid"),
        ("c", "Bob", "Madrid")
      ).toDF("record_id", "name", "city")
      val pairs = Core.findTrainingData(records, "record_id", Seq("name", "city"))
      val pairRows = pairs.collect()
      require(pairRows.length == 1, s"expected one candidate pair, got ${pairRows.length}")
      val outputPath = args.sliding(2, 1).collectFirst {
        case Array("--output-path", path) if path.nonEmpty => path.stripSuffix("/")
      }
      def persisted(df: org.apache.spark.sql.DataFrame, name: String): org.apache.spark.sql.DataFrame =
        outputPath.map(path => Core.persist(df, s"$path/$name")).getOrElse(df)

      val persistedPairs = persisted(pairs, "pairs")
      require(persistedPairs.count() == 1, "persisted candidate relation could not be read")
      val labeled = Core.label(persistedPairs, 0.5)
      require(labeled.select("z_isMatch").head().getInt(0) == 1, "label phase did not mark candidate")
      val persistedLabeled = persisted(labeled, "labeled")
      require(persistedLabeled.count() == 1, "persisted label relation could not be read")
      val explicit = pairs.select("z_cluster").limit(1).withColumn("z_isMatch", lit(0))
      val updated = Core.updateLabel(persistedLabeled, explicit)
      require(updated.select("z_isMatch").head().getInt(0) == 0, "updateLabel phase did not merge label")
      val persistedUpdated = persisted(updated, "updated")
      require(persistedUpdated.count() == 1, "persisted updated relation could not be read")
      val storage = outputPath.map(path => s" storage=$path").getOrElse("")
      println(s"ZINGG_NATIVE_SERVERLESS_CORE_E2E PASS similarities=exact,jaccard,jaro phases=findTrainingData,label,updateLabel persistence=${outputPath.isDefined} spark=${spark.version}$storage")
    } finally {
      spark.stop()
    }
  }
}
