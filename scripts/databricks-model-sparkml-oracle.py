"""Independent bounded Spark-ML oracle for the native model contract.

This is a validation task only. It does not participate in the Serverless
production artifact and uses no UDF or callback. The native model sidecar and
its scalar predictions are compared with the public Spark ML
VectorAssembler -> PolynomialExpansion -> LogisticRegression pipeline on the
same deterministic 16-row fixture.
"""
from __future__ import annotations

import argparse
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import PolynomialExpansion, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", required=True)
    args = parser.parse_args()
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    base = spark.range(0, 16)
    frame = base.select(
        F.col("id").alias("_probe_id"),
        *[(F.col("id") % F.lit(index + 3)).cast("double").alias(f"feature_{index}") for index in range(2)],
        F.when((F.col("id") % 2) == 0, F.lit(1.0)).otherwise(F.lit(0.0)).alias("label"),
    )
    features = ["feature_0", "feature_1"]
    assembler = VectorAssembler(inputCols=features, outputCol="feature_vector")
    expansion = PolynomialExpansion(degree=3, inputCol="feature_vector", outputCol="expanded_features")
    sidecar = spark.read.parquet(f"{args.model_path.rstrip('/')}/_zingg_native_model_v1").first()
    logistic = LogisticRegression(
        featuresCol="expanded_features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="raw_prediction",
        regParam=0.0001,
        elasticNetParam=0.0,
        maxIter=int(sidecar["maxIter"]),
        threshold=0.40,
        standardization=True,
        fitIntercept=True,
    )
    fitted = Pipeline(stages=[assembler, expansion, logistic]).fit(frame)
    oracle_model = fitted.stages[-1]
    oracle = fitted.transform(frame)

    native = spark.read.parquet(f"{args.model_path.rstrip('/')}/probe-prediction-rows-train")
    native_coefficients = [float(value) for value in sidecar["coefficients"]]
    oracle_coefficients = [float(value) for value in oracle_model.coefficients]
    if len(native_coefficients) != len(oracle_coefficients):
        raise RuntimeError(f"MODEL_ORACLE_TERM_COUNT_MISMATCH native={len(native_coefficients)} oracle={len(oracle_coefficients)}")
    coefficient_delta = max(abs(left - right) for left, right in zip(native_coefficients, oracle_coefficients))
    intercept_delta = abs(float(sidecar["intercept"]) - float(oracle_model.intercept))
    joined = native.alias("n").join(
        oracle.select("_probe_id", vector_to_array("probability").getItem(1).alias("oracle_score"), "prediction").alias("o"),
        F.col("n._probe_id") == F.col("o._probe_id"),
        "inner",
    )
    score_delta = joined.select(F.max(F.abs(F.col("n.score") - F.col("o.oracle_score")))).first()[0]
    prediction_mismatches = joined.where(F.col("n.prediction") != F.col("o.prediction")).count()
    score_delta = float(score_delta or 0.0)
    print(
        "NATIVE_MODEL_SPARKML_ORACLE "
        f"rows={joined.count()} terms={len(native_coefficients)} "
        f"coefficientMaxAbsDelta={coefficient_delta} interceptAbsDelta={intercept_delta} "
        f"scoreMaxAbsDelta={score_delta} predictionMismatches={prediction_mismatches}"
    )
    print(f"NATIVE_MODEL_SPARKML_ORACLE_NATIVE_COEFFICIENTS {native_coefficients}")
    print(f"NATIVE_MODEL_SPARKML_ORACLE_SPARKML_COEFFICIENTS {oracle_coefficients}")
    print(f"NATIVE_MODEL_SPARKML_ORACLE_NATIVE_INTERCEPT {float(sidecar['intercept'])}")
    print(f"NATIVE_MODEL_SPARKML_ORACLE_SPARKML_INTERCEPT {float(oracle_model.intercept)}")
    # Spark ML's public pipeline and the public-SQL replacement use different
    # optimizer implementations. Compare the resulting decision contract with
    # explicit tolerances rather than requiring byte-identical iterative paths.
    coefficient_tolerance = 0.35
    intercept_tolerance = 0.05
    score_tolerance = 0.01
    if (coefficient_delta > coefficient_tolerance or
            intercept_delta > intercept_tolerance or
            score_delta > score_tolerance or prediction_mismatches):
        raise RuntimeError(
            "NATIVE_MODEL_SPARKML_ORACLE_FAIL numerical or prediction parity exceeded "
            f"coefficient={coefficient_tolerance} intercept={intercept_tolerance} "
            f"score={score_tolerance}")
    print(
        "NATIVE_MODEL_SPARKML_ORACLE_PASS "
        f"coefficientTolerance={coefficient_tolerance} "
        f"interceptTolerance={intercept_tolerance} scoreTolerance={score_tolerance} "
        "predictionMismatches=0")


if __name__ == "__main__":
    main()
