"""Spark Connect transport boundary.

The server plugin must be installed on the Spark Connect server. A client-side
wheel or JAR is not sufficient and is intentionally rejected here.
"""

from typing import Any

from ..errors import BackendUnavailableError


class ConnectBackend:
    name = "connect-plugin"

    def __init__(self, spark: Any):
        self.spark = spark
        supported = False
        try:
            supported = bool(spark.conf.get("zingg.native.connect.plugin.loaded", "false"))
        except Exception:
            pass
        if not supported:
            raise BackendUnavailableError(
                "The zingg-native Spark Connect server plugin is not installed. "
                "Client-side Databricks Serverless wheel execution is not a substitute."
            )

    def transform(self, df: Any, operation: str, **options: Any) -> Any:
        raise NotImplementedError("ConnectBackend requires the installed server plugin protocol")
