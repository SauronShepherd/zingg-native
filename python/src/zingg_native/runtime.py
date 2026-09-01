"""Runtime/transport detection without pretending configuration is execution evidence."""
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RuntimeInfo:
    spark_version: str
    api_mode: str
    platform: str
    engine_candidate: str
    native_execution: bool | None = None


def detect_runtime(spark: Any) -> RuntimeInfo:
    version = str(getattr(spark, "version", "unknown"))
    conf = getattr(spark, "conf", None)
    def get(key: str, default: str = "") -> str:
        if conf is None:
            return default
        try:
            return str(conf.get(key, default))
        except (AttributeError, TypeError):
            return default
    module=f"{type(spark).__module__}.{type(spark).__name__}".lower()
    configured=get("spark.api.mode", "").lower()
    api_mode=configured if configured in {"classic","connect"} else ("connect" if "connect" in module else "classic")
    dbr=bool(get("spark.databricks.clusterUsageTags.sparkVersion") or get("spark.databricks.clusterUsageTags.clusterId") or "databricks" in module)
    photon=get("spark.databricks.photon.enabled", "").lower()=="true"
    fabric=get("spark.gluten.enabled", "").lower()=="true" or get("spark.native.execution.enabled", "").lower()=="true"
    platform="databricks" if dbr else ("fabric" if fabric else "apache-spark")
    candidate="photon" if dbr and photon else ("gluten-velox" if fabric else "spark")
    # Whether the executed query actually stayed native must come from supported
    # runtime/query-profile evidence, never from this configuration probe.
    return RuntimeInfo(version,api_mode,platform,candidate,None)
