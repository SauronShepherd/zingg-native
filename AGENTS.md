# Zingg Native working rules

- The sibling `../zingg` checkout is a read-only semantic reference.
- Supported runtime family is Spark 4.x with Scala 2.13; do not add Spark 3 or Scala 2.12 shims.
- This is an explicit facade/adapter, not Catalyst interception or plan rewriting.
- Keep algorithms in the shared Spark implementation; Python is API, configuration, and transport selection.
- Prefer built-in Spark expressions and higher-order functions. Do not use Python UDFs or opaque Scala UDFs for native replacements.
- Every operation must preserve Zingg 0.7 semantics, including null and edge-case behavior.
- Classic and Connect are first-class transports. Keep Py4J private handles isolated if a JVM backend is added.
- Do not claim Photon or Fabric NEE/Velox validation without evidence from those runtimes.
