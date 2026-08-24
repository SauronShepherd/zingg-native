"""Classic/Py4J transport for the shared public-expression rewrite registry."""
from __future__ import annotations
from typing import Any

from ..errors import BackendUnavailableError


def set_native_properties(spark: Any, mode: str, disabled_rules: tuple[str, ...] | list[str], run_id: str | None = None) -> None:
    """Set native policy on the Classic JVM transport when it is available."""
    system = spark._jvm.java.lang.System
    system.setProperty("zingg.native.mode", mode)
    system.setProperty("zingg.native.disabled.rules", ",".join(disabled_rules))
    if run_id:
        system.setProperty("zingg.native.run.id", run_id)


class ClassicBackend:
    name = "classic-py4j"
    expected_protocol = "1"
    expected_library_prefix = "0.3.0"

    def __init__(self, spark: Any):
        self.spark = spark
        try:
            gateway = spark._jvm.ai.zingg.native.gateway.ClassicGateway()
        except Exception as exc:
            raise BackendUnavailableError(
                "The zingg-native Scala core is not loaded in this Spark JVM; "
                "install the core JAR or use the patched Zingg artifact."
            ) from exc
        self._gateway = gateway
        if str(gateway.protocolVersion()) != self.expected_protocol:
            raise BackendUnavailableError(f"Unsupported zingg-native protocol: {gateway.protocolVersion()}")
        if not str(gateway.libraryVersion()).startswith(self.expected_library_prefix):
            raise BackendUnavailableError(f"Unsupported zingg-native library version: {gateway.libraryVersion()}")
        version = str(getattr(spark, "version", "unknown"))
        if version != "unknown" and not version.startswith("4."):
            raise BackendUnavailableError(f"Unsupported Spark runtime {version}; this build requires Spark 4.x")

    def transform(self, df: Any, operation: str, **options: Any) -> Any:
        right = options.get("right")
        if right is None:
            # Unary operations should use preprocess; this avoids an invented JVM null column name.
            raise ValueError("Classic transform requires a right column; use preprocess for unary rewrites")
        jdf = self._gateway.transform(df._jdf, operation, options["left"], right, options.get("output", "z_score"))
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def preprocess(self, df: Any, operation: str, columns: list[str], **parameters: Any) -> Any:
        if parameters:
            # Stop-word patterns are normally supplied by patched Zingg directly to the JVM provider.
            # Python Classic's tiny gateway intentionally carries no generic string parameter protocol.
            from .base import PublicExpressionBackend
            return PublicExpressionBackend(self.spark).preprocess(df, operation, columns, **parameters)
        java_columns = self.spark._jvm.java.util.ArrayList()
        for column in columns:
            java_columns.add(column)
        jdf = self._gateway.preprocess(df._jdf, operation, java_columns)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def capabilities(self) -> dict[str, Any]:
        return {
            "protocol_version": str(self._gateway.protocolVersion()),
            "metadata": str(self._gateway.capabilityMetadata()),
            "operations": list(self._gateway.supportedOperations()),
            "transport": self.name,
        }
