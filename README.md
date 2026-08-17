# Zingg Native

**ARCHITECTURE REMEDIATION IN PROGRESS — NOT RELEASE READY**

`zingg-native` is being rebuilt as a PySpark facade over a shared Scala 2.13
Spark core, with explicit Classic/Py4J and Spark Connect transports. The
existing Python-expression implementation is a preserved prototype, not the
release architecture.

## Current slice

The certified shared-core similarity slice is Exact, Jaccard, and Jaro through
Classic/Py4J. Self-managed Connect has verified Exact/Jaccard protocol/plugin
execution; Connect Jaro remains open.

```python
from zingg_native import Zingg

z = Zingg(spark=spark)
scored = z.exact(df, "first_name", "second_name")
```

Databricks Serverless shared-core JAR tasks and a parameterized Asset Bundle
have executed the certified similarity slice, three declarative phases, and
Unity Catalog Volume persistence on Spark 4.1.0. This does not prove that the
managed Connect server plugin can be activated; that path remains unsupported
on the tested environment. Reproduce the run with
[the Serverless runbook](docs/serverless-runbook.md) or
[the Asset Bundle guide](docs/bundle.md).
Fabric validation is deferred.

See [release-readiness.md](docs/release-readiness.md) for the evidence-based
support boundary.
