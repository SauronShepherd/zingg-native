# Databricks target

Both Databricks Dedicated + Photon and Databricks Serverless are mandatory targets.

## Dedicated + Photon

Build the core with a Dedicated Spark 4 profile, patch/build Zingg 0.7.0 with the native-core dependency, install both JARs, and launch the ordinary Zingg main class. The overlay redirects non-native Spark construction boundaries into the shared registry. Py4J remains available for Python/JVM transport where the surrounding Zingg application uses it; it is not the rewrite mechanism.

## Serverless

Build with `-Pdatabricks-serverless-env5`. The production core and launcher use public Spark/DataFrame APIs and the Databricks Connect compile-time API; they do not require Catalyst internals or a managed custom Connect server extension. The launcher creates the managed Spark session, sets it as active/default, consumes only adapter flags, and delegates all ordinary arguments to the real patched Zingg main class. Native execution also bypasses upstream cache calls and replaces the Scala Spark-ML training boundary with the public-DataFrame native model stage.

## Native policy

Dedicated/local execution can use `-Dzingg.native.mode=STRICT` (or local environment fallback). Serverless uses `--native-mode STRICT`; optional launcher settings are `--native-run-id`, `--native-disabled-rules`, and `--native-plan-guard`. No successful execution is treated as Photon proof by the code; Photon evidence is a separate validation concern.

Serverless environment 5 has runtime evidence for the real Zingg 0.7.0
`findTrainingData` path and a strict disabled-rule failure. This does not certify
Photon execution: authoritative Databricks Query Profile/operator evidence and
Dedicated validation remain outstanding.
