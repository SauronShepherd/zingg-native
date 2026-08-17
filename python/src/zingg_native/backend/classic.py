"""Classic/Py4J transport for the shared Scala core."""

from typing import Any

from ..errors import BackendUnavailableError


class ClassicBackend:
    name = "classic-py4j"

    def __init__(self, spark: Any):
        self.spark = spark
        try:
            gateway = spark._jvm.ai.zingg.native.gateway.ClassicGateway()
            self._gateway = gateway
        except Exception as exc:
            raise BackendUnavailableError(
                "The shared zingg-native Scala core is not loaded in this Spark JVM; "
                "install the core JAR before using backend='classic'."
            ) from exc
        if gateway.protocolVersion() != "1":
            raise BackendUnavailableError(f"Unsupported zingg-native protocol: {gateway.protocolVersion()}")

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

    def capabilities(self) -> dict[str, Any]:
        return {
            "metadata": self._gateway.capabilityMetadata(),
            "operations": list(self._gateway.supportedOperations()),
            "phases": list(self._gateway.supportedPhases()),
        }

    def find_training_data(self, df: Any, keys: list[str], id_column: str, output_path: str | None = None) -> Any:
        if not keys:
            raise ValueError("keys must contain at least one column")
        java_keys = self.spark._jvm.java.util.ArrayList()
        for key in keys:
            java_keys.add(key)
        jdf = self._gateway.findTrainingData(df._jdf, id_column, java_keys)
        if output_path:
            jdf = self._gateway.persist(jdf, output_path)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def label(self, df: Any, threshold: float, output_path: str | None = None) -> Any:
        jdf = self._gateway.label(df._jdf, float(threshold))
        if output_path:
            jdf = self._gateway.persist(jdf, output_path)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def update_label(self, pairs: Any, labels: Any, output_path: str | None = None) -> Any:
        jdf = self._gateway.updateLabel(pairs._jdf, labels._jdf)
        if output_path:
            jdf = self._gateway.persist(jdf, output_path)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)
