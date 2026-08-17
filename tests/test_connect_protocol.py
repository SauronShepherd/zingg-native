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

    Expression = pytest.importorskip("pyspark.sql.connect.expressions").Expression

    assert isinstance(_ZinggNativeExpression("EXACT_SIMILARITY", []), Expression)


def test_connect_rejects_unimplemented_jaro_operation():
    from zingg_native.backend.connect import ConnectBackend

    class Conf:
        def get(self, key, default):
            return "true"

    class Spark:
        conf = Conf()

    backend = ConnectBackend(Spark())
    try:
        backend.transform(None, "JARO_SIMILARITY", left="left", right="right")
    except NotImplementedError as exc:
        assert "JARO_SIMILARITY" in str(exc)
    else:
        raise AssertionError("Connect must reject unsupported Jaro")
