# Zingg Native

**ARCHITECTURE REMEDIATION IN PROGRESS — NOT RELEASE READY**

`zingg-native` is being rebuilt as a PySpark facade over a shared Scala 2.13
Spark core, with explicit Classic/Py4J and Spark Connect transports. The
existing Python-expression implementation is a preserved prototype, not the
release architecture.

## Current slice

The first native operation is `EXACT_SIMILARITY`, including Zingg's unusual
null behavior. It works through Spark Classic and Spark Connect because both
consume the same declarative expression plan.

```python
from zingg_native import Zingg

z = Zingg(spark=spark)
scored = z.exact(df, "first_name", "second_name")
```

Earlier wheel-only Databricks Serverless runs validated expression feasibility
only. They do not validate the required shared Scala core or Connect server
plugin, so Databricks Serverless is not currently supported or claimable.
Fabric validation is deferred.

See [release-readiness.md](docs/release-readiness.md) for the evidence-based
support boundary.
