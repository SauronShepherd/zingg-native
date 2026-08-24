package ai.zingg.native.tools

import ai.zingg.native.{NativeModelEngine, NativeOperationProvider, NativeTrainedModel}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{PolynomialExpansion, VectorAssembler}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession

/**
 * Dedicated/local migration utility for Zingg 0.7 CrossValidatorModel assets.
 *
 * It never participates in Serverless execution.  It reads the ordinary Spark
 * ML model once where Spark ML model loading is available and writes the
 * versioned zingg-native sidecar consumed by REWRITE/STRICT prediction.
 * Existing legacy files are left untouched.
 *
 * Usage:
 *   LegacyCrossValidatorModelConverter <zingg-model-path>
 */
object LegacyCrossValidatorModelConverter {
  def main(args: Array[String]): Unit = {
    if (args.length != 1 || args(0).trim.isEmpty)
      throw new IllegalArgumentException("Usage: LegacyCrossValidatorModelConverter <zingg-model-path>")

    val spark = SparkSession.builder().getOrCreate()
    convert(spark, args(0).trim)
  }

  def convert(spark: SparkSession, path: String): Unit = {
    val legacy = CrossValidatorModel.load(path)
    val pipeline = legacy.bestModel match {
      case value: PipelineModel => value
      case other => throw new IllegalStateException(
        s"Expected Zingg CrossValidator bestModel to be PipelineModel, got ${other.getClass.getName}")
    }

    val assembler = pipeline.stages.collectFirst { case value: VectorAssembler => value }.getOrElse {
      throw new IllegalStateException("Legacy Zingg model does not contain VectorAssembler")
    }
    val polynomial = pipeline.stages.collectFirst { case value: PolynomialExpansion => value }.getOrElse {
      throw new IllegalStateException("Legacy Zingg model does not contain PolynomialExpansion")
    }
    val logistic = pipeline.stages.collectFirst { case value: LogisticRegressionModel => value }.getOrElse {
      throw new IllegalStateException("Legacy Zingg model does not contain LogisticRegressionModel")
    }
    if (polynomial.getDegree != NativeModelEngine.PolynomialDegree)
      throw new IllegalStateException(
        s"Legacy model degree ${polynomial.getDegree} is not the Zingg 0.7 degree ${NativeModelEngine.PolynomialDegree}")

    val provider = NativeOperationProvider.fromSpark(spark, "model.legacyConversion")
    val artifact = NativeTrainedModel(
      NativeModelEngine.SchemaVersion,
      assembler.getInputCols.toVector,
      polynomial.getDegree,
      NativeModelEngine.PolynomialOrdering,
      logistic.coefficients.toArray.toVector,
      logistic.intercept,
      logistic.getRegParam,
      logistic.getThreshold,
      logistic.getMaxIter,
      legacy.getNumFolds,
      legacy.getSeed,
      "imported-spark-logistic-regression")

    NativeModelEngine.save(spark, artifact, path, provider.context)
  }
}
