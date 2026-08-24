# Native Zingg model stage

`SparkModel` remains the model class created and called by Zingg 0.7.0. In `REWRITE` and `STRICT` mode its Spark-specific training boundary is redirected to `NativeModelEngine`.

The native stage keeps the upstream similarity feature columns and order, reproduces Spark `PolynomialExpansion(degree=3)` term ordering, uses the upstream regularization and threshold grids, two-fold selection and 100 training iterations, and emits Zingg's existing score/prediction column names. Training is expressed as Spark SQL/DataFrame projections, aggregates, filters and windows rather than a distributed Spark ML estimator.

Native persistence is a versioned Parquet sidecar under the ordinary Zingg model directory (`_zingg_native_model_v1`). The sidecar stores feature order, polynomial contract, coefficients, intercept, selected regularization, threshold, folds, seed and optimizer contract. Legacy `CrossValidatorModel` files are left untouched in OFF/AUDIT mode.

For an existing Zingg 0.7 model, build the optional `-Plegacy-model-converter` module and run `ai.zingg.native.tools.LegacyCrossValidatorModelConverter` on Dedicated/local Spark where ordinary Spark ML model loading is available. It extracts the best fitted pipeline and writes the native sidecar into the same model directory. Serverless can then load the sidecar without loading the legacy Spark ML model.

The native model path is built and exercised indirectly by the Databricks
Serverless `findTrainingData` validation. Full model train/predict parity,
persistence round trips, and Dedicated Photon validation remain release gates;
the current runtime evidence does not certify exhaustive upstream numerical
parity. Local JUnit coverage now pins the two-feature Spark polynomial ordering
contract, production regularization/threshold grids, degree-3 term count, and
deterministic path generation; Serverless evidence separately proves bounded
fit/save/load/predict and the full production train path on recorded releases.
