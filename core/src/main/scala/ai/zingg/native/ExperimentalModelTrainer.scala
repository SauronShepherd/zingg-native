package ai.zingg.native

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Experimental model-fit seam for the eventual upstream-compatible trainer.
  *
  * The caller must provide already-constructed declarative feature columns and
  * a blocking-tree reference. This deliberately does not claim to implement
  * Zingg's preprocessing or blocking-tree learner.
  */
object ExperimentalModelTrainer {
  def fit(
      labeled: DataFrame,
      featureColumns: Seq[String],
      modelPath: String,
      modelChecksum: String,
      blockingTree: BlockingTreeArtifact
  ): ModelArtifact = {
    require(featureColumns.nonEmpty, "feature columns must be non-empty")
    ArtifactSchema.validatePath(modelPath)
    require(modelChecksum != null && modelChecksum.nonEmpty, "model checksum must be non-empty")
    val evidence = Core.inspectTrainingEvidence(labeled)
    require(evidence.isSufficient, s"insufficient training evidence: $evidence")
    val missing = (featureColumns :+ "z_isMatch").distinct.filterNot(labeled.columns.contains)
    require(missing.isEmpty, s"unknown training columns: ${missing.mkString(", ")}")

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol("_zingg_native_features")
    val training = assembler.transform(labeled)
      .select(col("_zingg_native_features").alias("features"), col("z_isMatch").cast("double").alias("label"))
    val model = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setMaxIter(100)
      .fit(training)
    model.write.overwrite().save(modelPath)
    ModelArtifact(1, "SPARK_LOGISTIC_REGRESSION_EXPERIMENTAL", modelPath, modelChecksum,
      featureColumns, evidence.positivePairs, evidence.negativePairs, blockingTree)
  }
}
