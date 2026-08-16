"""Runtime detection and capability reporting."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RuntimeInfo:
    spark_version: str
    api_mode: str
    engine: str
    native_execution: bool


def detect_runtime(spark: Any) -> RuntimeInfo:
    """Inspect public Spark configuration where possible; never infer support."""
    version = getattr(spark, "version", "unknown")
    conf = getattr(spark, "conf", None)

    def get(key: str, default: str = "") -> str:
        try:
            return str(conf.get(key, default))
        except Exception:
            return default

    configured_api_mode = get("spark.api.mode", "").lower()
    session_module = f"{type(spark).__module__}.{type(spark).__name__}".lower()
    if configured_api_mode in {"classic", "connect"}:
        api_mode = configured_api_mode
    elif "connect" in session_module:
        api_mode = "connect"
    else:
        api_mode = "classic"
    vendor = " ".join((get("spark.databricks.clusterUsageTags.sparkVersion"),
                       get("spark.databricks.photon.enabled"))).lower()
    fabric = " ".join((get("spark.gluten.enabled"), get("spark.native.execution.enabled"))).lower()
    if "true" in vendor and "photon" in vendor:
        engine, native = "photon", True
    elif "true" in fabric:
        engine, native = "gluten-velox", True
    else:
        engine, native = "spark", False
    return RuntimeInfo(str(version), api_mode, engine, native)
