"""Transport selection. Algorithms live in the shared JVM core."""

from typing import Any, Protocol


class ExecutionBackend(Protocol):
    name: str

    def transform(self, df: Any, operation: str, **options: Any) -> Any: ...


class PrototypeExpressionBackend:
    """Opt-in comparison prototype; never selected by default."""
    name = "spark-expressions"

    def transform(self, df: Any, operation: str, **options: Any) -> Any:
        from ..similarity import exact, jaccard_similarity, jaro_similarity
        if operation == "EXACT_SIMILARITY":
            return exact(df, options["left"], options["right"], options.get("output", "z_exact"))
        if operation == "JACCARD_SIMILARITY":
            return df.withColumn(options.get("output", "z_jaccard"), jaccard_similarity(options["left"], options["right"]))
        if operation == "JARO_SIMILARITY":
            return df.withColumn(options.get("output", "z_jaro"), jaro_similarity(options["left"], options["right"]))
        if operation not in ("EXACT_SIMILARITY", "JACCARD_SIMILARITY", "JARO_SIMILARITY"):
            raise NotImplementedError(f"Unsupported native operation: {operation}")


def resolve_backend(spark: Any, requested: str | None = None) -> ExecutionBackend:
    """Resolve an explicit transport; prototype expressions require opt-in."""
    if requested is None:
        from ..runtime import detect_runtime
        mode = detect_runtime(spark).api_mode
    else:
        mode = requested
    if mode == "classic":
        from .classic import ClassicBackend
        return ClassicBackend(spark)
    if mode == "connect":
        from .connect import ConnectBackend
        return ConnectBackend(spark)
    if mode == "expressions":
        return PrototypeExpressionBackend()
    raise ValueError("backend must be classic, connect, or expressions")
