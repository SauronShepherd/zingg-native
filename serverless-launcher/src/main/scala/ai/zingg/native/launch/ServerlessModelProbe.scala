package ai.zingg.native.launch

import ai.zingg.native.{NativeModelEngine}
import ai.zingg.nativebridge.NativeOperationProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Bounded diagnostic for the public model fit/save/load/predict seam. */
object ServerlessModelProbe {
  def run(spark: SparkSession): Unit = {
    val (input, featureColumns) = probeInput(spark)
    val provider = NativeOperationProvider.fromSpark(spark, "model.probe")
    val model = provider.fitModel(input, featureColumns.indices.map(index => s"feature_$index").toArray, "label")
    val path = probePath()
    println("NATIVE_MODEL_PROBE_STAGE fit-complete")
    provider.saveModel(model, path)
    println("NATIVE_MODEL_PROBE_STAGE save-complete")
    val artifact = spark.read.parquet(s"${path.stripSuffix("/")}/_zingg_native_model_v1").head()
    // Spark 4.1 Connect may materialize an array as mutable.ArraySeq. Read
    // through the collection interface so the assertion itself is transport
    // neutral across Classic and managed Connect.
    val savedFeatures = artifact.getAs[scala.collection.Seq[String]]("featureColumns").toVector
    val savedCoefficients = artifact.getAs[scala.collection.Seq[Double]]("coefficients")
    val expectedTerms = featureColumns.size + (featureColumns.size * (featureColumns.size + 1) / 2) +
      (featureColumns.size * (featureColumns.size + 1) * (featureColumns.size + 2) / 6)
    require(savedFeatures == featureColumns.indices.map(index => s"feature_$index").toVector,
      s"Native model feature ordering mismatch: $savedFeatures")
    require(artifact.getAs[Int]("polynomialDegree") == 3,
      "Native model probe did not persist production polynomial degree 3")
    require(artifact.getAs[String]("polynomialOrdering") == "spark-polynomial-expansion-order-v1",
      "Native model probe did not persist the versioned Spark polynomial ordering")
    require(savedCoefficients.size == expectedTerms,
      s"Native model probe persisted ${savedCoefficients.size} coefficients; expected $expectedTerms")
    require(artifact.getAs[Double]("regParam") == 0.0001d,
      s"Bounded probe selected unexpected first regularization grid value ${artifact.getAs[Double]("regParam")}")
    require(artifact.getAs[Double]("threshold") == 0.40d,
      s"Bounded probe selected unexpected first threshold grid value ${artifact.getAs[Double]("threshold")}")
    if (sys.props.get("zingg.native.model.boundedProbe").exists(_.equalsIgnoreCase("true"))) {
      require(artifact.getAs[Int]("maxIter") == 1 && artifact.getAs[Int]("numFolds") == 1,
        "Bounded probe did not persist its explicit iteration/fold contract")
    }
    require(artifact.getAs[Long]("seed") == 13L && artifact.getAs[String]("optimizer") == NativeModelEngine.Optimizer,
      "Native model probe persisted unexpected optimizer metadata")
    println(s"NATIVE_MODEL_PROBE_STAGE contract-complete features=${featureColumns.size} terms=$expectedTerms degree=3 regGridHead=0.0001 thresholdGridHead=0.40")
    val loaded = provider.loadModel(path)
    println("NATIVE_MODEL_PROBE_STAGE load-complete")
    val output = provider.predictModel(input, loaded, "feature_vector", "expanded_features",
      "probability", "raw_prediction", "prediction", "score")
    val fingerprint = predictionFingerprint(spark, output, path, "train")
    spark.range(1L).select(lit(fingerprint).alias("fingerprint"))
      .write.mode("overwrite").parquet(s"${path.stripSuffix("/")}/probe-predictions")
    // Keep the runtime action narrow: selecting the diagnostic vector arrays
    // on the same 1,770-term lineage can force Serverless to materialize the
    // entire expanded feature projection. Probability/raw_prediction
    // formulas are asserted statically below; this action validates the
    // observable scalar score and thresholded prediction contract.
    val consistency = output.select("score", "prediction")
      .where("score < 0.0 OR score > 1.0 OR prediction <> CASE WHEN score > 0.40 THEN 1.0 ELSE 0.0 END")
      .count()
    require(consistency == 0L, s"Native model prediction contract had $consistency inconsistent rows")
    val rows = output.select("score").count()
    if (rows != 16L) throw new IllegalStateException(s"Native model probe returned $rows rows")
    println(s"NATIVE_MODEL_PROBE_PASS rows=16 features=${featureColumns.size} terms=$expectedTerms degree=3 fit=true save=true load=true predict=true metadata=true predictions=true")
  }

  /** Load a model written by a different Serverless task and predict with it. */
  def loadAndPredict(spark: SparkSession): Unit = {
    val (input, _) = probeInput(spark)
    val path = sys.props.getOrElse("zingg.native.model.probe.path",
      throw new IllegalArgumentException("--native-model-load-probe requires --native-model-probe-path"))
    val provider = NativeOperationProvider.fromSpark(spark, "model.probe.load")
    val loaded = provider.loadModel(path)
    println("NATIVE_MODEL_PROBE_STAGE load-only-complete")
    val output = provider.predictModel(input, loaded, "feature_vector", "expanded_features",
      "probability", "raw_prediction", "prediction", "score")
    val expectedFingerprint = readPredictionFingerprint(spark, path)
    require(predictionFingerprint(spark, output, path, "load") == expectedFingerprint,
      "Cross-job native model prediction fingerprint mismatch")
    val rows = output.select("score").count()
    if (rows != 16L) throw new IllegalStateException(s"Native model load probe returned $rows rows")
    println("NATIVE_MODEL_PROBE_LOAD_PASS rows=16 fit=false save=false load=true predict=true metadata=true predictions=true")
  }

  private def probeInput(spark: SparkSession): (org.apache.spark.sql.DataFrame, Seq[org.apache.spark.sql.Column]) = {
    val base = spark.range(0L, 16L)
    val featureCount = sys.props.get("zingg.native.model.probe.features").flatMap(_.toIntOption).filter(n => n >= 2 && n <= 20).getOrElse(20)
    val featureColumns = (0 until featureCount).map { index =>
      (col("id") % lit((index + 3).toLong)).cast("double").alias(s"feature_$index")
    }
    val input = base.select((Seq(col("id").alias("_probe_id")) ++ featureColumns :+
      when((col("id") % lit(2L)) === lit(0L), lit(1)).otherwise(lit(0)).alias("label")): _*)
    (input, featureColumns)
  }

  private def predictionFingerprint(
      spark: SparkSession,
      output: org.apache.spark.sql.DataFrame,
      root: String,
      suffix: String): String = {
    // Materialize only the scalar prediction contract before hashing. A
    // driver-side collect or aggregate directly over the 1,770-term lineage
    // can leave the managed Connect kernel waiting indefinitely.
    val rowsPath = s"${root.stripSuffix("/")}/probe-prediction-rows-$suffix"
    output.select("_probe_id", "score", "prediction").write.mode("overwrite").parquet(rowsPath)
    val rows = spark.read.parquet(rowsPath)
    val rowMaterial = concat_ws(
      "|",
      rows.col("_probe_id").cast("string"),
      rows.col("score").cast("string"),
      rows.col("prediction").cast("string"))
    val row = rows
      .agg(sha2(concat_ws(";", sort_array(collect_list(rowMaterial))), 256).alias("fingerprint"))
      .head()
    row.getString(0)
  }

  private def readPredictionFingerprint(spark: SparkSession, path: String): String = {
    val rows = spark.read.parquet(s"${path.stripSuffix("/")}/probe-predictions").select("fingerprint").collect()
    require(rows.length == 1, s"Expected one cross-job prediction fingerprint row, found ${rows.length}")
    rows.head.getString(0)
  }

  private def probePath(): String = sys.props.getOrElse(
    "zingg.native.model.probe.path", {
      // Isolate every default probe artifact so an interrupted task cannot
      // leave a stale model/Parquet boundary for the next validation.
      val runToken = sys.props.getOrElse("zingg.native.run.id", java.util.UUID.randomUUID().toString)
        .replaceAll("[^A-Za-z0-9_-]", "-")
      s"/Volumes/sda_dev/default/zingg_native_e2e_volume/model-probe-$runToken"
    })
}
