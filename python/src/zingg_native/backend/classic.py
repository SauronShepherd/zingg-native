"""Classic/Py4J transport for the shared Scala core."""

from typing import Any


class ClassicBackend:
    name = "classic-py4j"

    def __init__(self, spark: Any):
        self.spark = spark
        try:
            gateway = spark._jvm.ai.zingg.native.gateway.ClassicGateway()
            self._gateway = gateway
        except Exception as exc:
            raise RuntimeError(
                "The shared zingg-native Scala core is not loaded in this Spark JVM; "
                "install the core JAR before using backend='classic'."
            ) from exc
        if gateway.protocolVersion() != "1":
            raise RuntimeError(f"Unsupported zingg-native protocol: {gateway.protocolVersion()}")

    def transform(self, df: Any, operation: str, **options: Any) -> Any:
        jdf = self._gateway.transform(
            df._jdf,
            operation,
            options["left"],
            options["right"],
            options.get("output", "z_score"),
        )
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)
