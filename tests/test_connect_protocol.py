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
