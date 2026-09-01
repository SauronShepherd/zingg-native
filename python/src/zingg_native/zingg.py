"""Thin Python control surface for the zingg-native adapter.

This module intentionally does *not* implement Zingg phases.  The production
path is the patched upstream Zingg 0.7 Spark code, whose Spark-specific choke
points call ``ai.zingg.nativebridge.NativeOperationProvider``.  The Python
class below is only a transport-neutral way to activate policy, inspect
capabilities. It does not construct semantic rewrites; ordinary Zingg remains
JVM-owned.
"""
from __future__ import annotations

from typing import Any

from .adapter import activate
from .backend import resolve_backend
from .config import NativeConfig
from .runtime import detect_runtime


class Zingg:
    """Compatibility name for the thin native adapter, not a Zingg engine."""

    library_version = "0.3.0-SNAPSHOT"

    def __init__(
        self,
        arguments: Any = None,
        spark: Any = None,
        backend: str | None = None,
        config: NativeConfig | None = None,
    ) -> None:
        if spark is None:
            raise ValueError("spark is required; create a Spark 4 Classic or Connect session")
        self.arguments = arguments  # retained only for source compatibility
        self.spark = spark
        self.config = config or NativeConfig()
        self.runtime = detect_runtime(spark)
        self.backend = resolve_backend(spark, backend)
        activate(spark, self.config)

    def status(self) -> dict[str, Any]:
        """Return transport/configuration diagnostics, never Photon proof."""
        result: dict[str, Any] = {
            "library_version": self.library_version,
            "protocol_version": self.config.protocol_version,
            "native_mode": self.config.mode,
            "backend": getattr(self.backend, "name", type(self.backend).__name__),
            "spark_version": self.runtime.spark_version,
            "api_mode": self.runtime.api_mode,
            "platform": self.runtime.platform,
            "engine_candidate": self.runtime.engine_candidate,
            "native_execution_observed": self.runtime.native_execution,
        }
        capabilities = getattr(self.backend, "capabilities", None)
        if capabilities is not None:
            result["capabilities"] = capabilities()
        return result

    def transform(self, df: Any, operation: str, **options: Any) -> Any:
        """Reject direct Python rewrites; ordinary Zingg is JVM-owned."""
        return self.backend.transform(df, operation, **options)

    def preprocess(
        self,
        df: Any,
        operation: str,
        columns: list[str],
        **parameters: Any,
    ) -> Any:
        """Reject direct Python preprocessing; ordinary Zingg is JVM-owned."""
        method = getattr(self.backend, "preprocess", None)
        if method is None:
            raise NotImplementedError(f"backend {getattr(self.backend, 'name', self.backend)!r} has no preprocess surface")
        return method(df, operation, columns, **parameters)

    def exact(self, df: Any, left: str, right: str, output: str = "z_exact") -> Any:
        return self.transform(df, "similarity.SimilarityFunctionExact", left=left, right=right, output=output)

    def jaccard(self, df: Any, left: str, right: str, output: str = "z_jaccard") -> Any:
        return self.transform(df, "similarity.JaccSimFunction", left=left, right=right, output=output)

    def jaro(self, df: Any, left: str, right: str, output: str = "z_jaro") -> Any:
        return self.transform(df, "similarity.JaroWinklerFunction", left=left, right=right, output=output)


NativeAdapter = Zingg
