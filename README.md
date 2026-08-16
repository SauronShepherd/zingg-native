# Zingg Native

**ARCHITECTURE REMEDIATION IN PROGRESS — NOT RELEASE READY**

`zingg-native` is being rebuilt as a PySpark facade over a shared Scala 2.13
Spark core, with explicit Classic/Py4J and Spark Connect transports. The
existing Python-expression implementation is a preserved prototype, not the
release architecture.

## Current slice

The certified shared-core similarity slice is Exact, Jaccard, and Jaro through
Classic/Py4J. Connect currently has an Exact protocol/plugin build path only;
its server execution is not yet claimed.

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
