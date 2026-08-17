package ai.zingg.serverless

import ai.zingg.native.Core
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/** Complete public-Spark-API phase gate packaged in the common Serverless JAR. */
object ServerlessCoreE2E {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      import spark.implicits._
      val output = args.sliding(2, 1).collectFirst { case Array("--output-path", p) => p.stripSuffix("/") }
      def save(df: org.apache.spark.sql.DataFrame, name: String) = output.map(p => Core.persist(df, s"$p/$name")).getOrElse(df)
      val records = Seq(("r1", " Alice ", "Madrid"), ("r2", "alice", "madrid"), ("r3", "Bob", "Barcelona"))
        .toDF("record_id", "name", "city")
      val normalized = Core.preprocess(Core.preprocess(records, "TRIM", Seq("name", "city")), "CASE_NORMALIZE", Seq("name", "city"))
      val candidates = save(Core.findTrainingData(normalized, "record_id", Seq("name", "city")), "findTrainingData")
      require(candidates.count() == 1, "findTrainingData did not produce the expected candidate")
      val labeled = save(Core.label(candidates, 0.5), "label")
      val labels = labeled.select("z_cluster").withColumn("z_isMatch", lit(1))
      val updated = save(Core.updateLabel(labeled, labels), "updateLabel")
      val trainingRecords = (Seq.fill(5)(1) ++ Seq.fill(5)(0)).zipWithIndex.flatMap { case (label, i) =>
        Seq((s"$i-a", s"c$i", label), (s"$i-b", s"c$i", label))
      }.toDF("record_id", "z_cluster", "z_isMatch")
      val trainingPairs = save(Core.buildTrainingPairs(trainingRecords, "record_id"), "train")
      val evidence = Core.inspectTrainingEvidence(trainingPairs)
      require(evidence.isSufficient, s"train evidence insufficient: $evidence")
      val modelRows = (Seq.fill(5)((1.0, 1.0, 1)) ++ Seq.fill(5)((0.0, 0.0, 0)))
        .toDF("z_name", "z_city", "z_isMatch")
      val modelPath = output.map(p => s"$p/model")
      val model = Core.trainModel(modelRows, Seq("z_name", "z_city"), modelPath = modelPath)
      val scored = Core.matchModel(modelRows.drop("z_isMatch"), model, Seq("z_name", "z_city"))
      require(scored.select("z_score").count() == 10, "model scoring did not produce all rows")
      val linked = Core.linkComponents(trainingPairs, "z_left_record_id", "z_right_record_id")
      require(linked.count() == 20, "link phase did not return all graph vertices")
      save(linked, "link")
      println(s"ZINGG_NATIVE_SERVERLESS_FULL_E2E PASS phases=preprocess,findTrainingData,label,updateLabel,train,match,link trainingEvidence=${evidence.positivePairs}/${evidence.negativePairs} model=logistic-regression persistence=${output.isDefined} spark=${spark.version}")
    } finally spark.stop()
  }
}
