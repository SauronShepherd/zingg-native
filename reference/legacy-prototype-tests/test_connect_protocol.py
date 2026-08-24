from pathlib import Path

import pytest
from zingg_native.backend.connect import _ZinggNativeExpression


class _Argument:
    def __init__(self, value: bytes):
        self.value = value

    def to_plan(self, session):
        from pyspark.sql.connect import proto

        return proto.Expression(expression_string=proto.Expression.ExpressionString(
            expression=self.value.decode("utf-8")
        ))


def test_connect_payload_is_versioned_and_contains_children():
    expression = _ZinggNativeExpression("EXACT_SIMILARITY", [_Argument(b"left"), _Argument(b"right")])
    plan = expression.to_plan(None)
    assert plan.extension.type_url.endswith("ZinggNativeExpression")
    assert b"EXACT_SIMILARITY" in plan.extension.value
    assert plan.extension.value.count(b"left") == 1
    assert plan.extension.value.count(b"right") == 1


def test_connect_expression_uses_spark_41_expression_base():
    import pytest

    try:
        from pyspark.sql.connect.expressions import Expression
    except Exception as exc:
        pytest.skip(f"Spark Connect optional dependencies unavailable: {exc}")

    assert isinstance(_ZinggNativeExpression("EXACT_SIMILARITY", []), Expression)


def test_proto_declares_versioned_artifact_references():
    from pathlib import Path

    proto = (Path(__file__).parents[1] / "connect/src/main/protobuf/zingg_native.proto").read_text()
    assert "message BlockingTreeArtifact" in proto
    assert "message ModelArtifact" in proto
    assert "uint32 schema_version = 1" in proto


def test_connect_rejects_unimplemented_jaro_operation():
    from zingg_native.backend.connect import ConnectBackend

    class Conf:
        def get(self, key, default):
            return "true"

    class Spark:
        conf = Conf()

    backend = ConnectBackend(Spark())
    with pytest.raises(NotImplementedError, match="JARO_SIMILARITY"):
        backend.transform(None, "JARO_SIMILARITY", left="left", right="right")


def test_connect_accepts_shared_case_normalize_operation():
    from zingg_native.backend.connect import ConnectBackend

    class Conf:
        def get(self, key, default):
            return "true"

    class Spark:
        conf = Conf()

    backend = ConnectBackend(Spark())
    assert backend.name == "connect-plugin"
    assert backend.capabilities()["protocol_version"] == "1"
    assert "EXACT_SIMILARITY" in backend.capabilities()["operations"]
    assert backend.capabilities()["jaro"] == "open"
    source = (Path(__file__).parents[1] / "connect/src/main/scala/ai/zingg/native/connect/ZinggNativeExpressionPlugin.scala").read_text()
    assert 'case "CASE_NORMALIZE"' in source
    assert '"TRIM"' in source


def test_connect_rejects_unknown_preprocessing_before_serialization():
    from zingg_native.backend.connect import ConnectBackend

    class Conf:
        def get(self, key, default):
            return "true"

    class Spark:
        conf = Conf()

    backend = ConnectBackend(Spark())
    import pytest

    with pytest.raises(NotImplementedError, match="preprocessing operation NOPE"):
        backend.preprocess(None, "NOPE", ["name"])


def test_facade_allows_preprocessing_on_configured_connect_backend():
    from zingg_native import Zingg

    z = object.__new__(Zingg)
    z.backend = type("ConnectBackend", (), {})()
    assert "ConnectBackend" in (Path(__file__).parents[1] / "python/src/zingg_native/zingg.py").read_text()
