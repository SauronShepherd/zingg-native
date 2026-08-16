# Architecture

The remediation target is a Python API over one Scala 2.13 Spark core:

```text
Python facade -> ClassicBackend -> Py4J -> core JAR -> Spark SQL expressions
Python facade -> ConnectBackend -> Connect server plugin -> same core JAR
```

The core currently contains the certified Exact and Jaccard expression
implementations and a Java/Py4J-friendly `ClassicGateway`. The Connect module
contains the Spark 4.1 `ExpressionPlugin` entry point and versioned protocol
schema. Its non-empty payload path is deliberately gated until generated
protocol parsing and child-expression conversion are complete.

The old Python expression path is available only with `backend="expressions"`
for comparison. It is not the production transport and is not Databricks
architecture evidence.
