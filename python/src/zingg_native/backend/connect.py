"""Spark Connect transport boundary.

The server plugin must be installed on the Spark Connect server. A client-side
wheel or JAR is not sufficient and is intentionally rejected here.
"""

from typing import Any, Sequence

from ..errors import BackendUnavailableError


def _varint(value: int) -> bytes:
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _bytes_field(number: int, value: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(value)) + value


class _ZinggNativeExpression:
    """Spark Connect expression envelope; arithmetic remains in the JVM core."""

    _type_url = "type.googleapis.com/ai.zingg.native.connect.v1.ZinggNativeExpression"

    def __init__(self, operation: str, arguments: Sequence[Any]):
        self.operation = operation
        self.arguments = arguments

    def to_plan(self, session: Any) -> Any:
        from google.protobuf.any_pb2 import Any as ProtoAny
        from pyspark.sql.connect import proto

        payload = _varint(8) + _varint(1) + _bytes_field(2, self.operation.encode("utf-8"))
        for argument in self.arguments:
            payload += _bytes_field(3, argument.to_plan(session).SerializeToString())
        extension = ProtoAny(type_url=self._type_url, value=payload)
        return proto.Expression(extension=extension)


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
        if operation not in {"EXACT_SIMILARITY"}:
            raise NotImplementedError(
                f"Connect server plugin does not certify operation {operation}"
            )
        from pyspark.sql.connect.column import Column

        expression = _ZinggNativeExpression(
            operation,
            [df[options["left"]]._expr, df[options["right"]]._expr],
        )
        return df.withColumn(options.get("output", "z_exact"), Column(expression))
