package ai.zingg.native.connect

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.ExpressionPlugin
import java.util.Optional

/**
 * Server-side extension entry point. The actual expression conversion is
 * deliberately gated until the versioned payload and child-expression
 * contract are present; unknown payloads must fall through to Spark rather
 * than silently changing semantics.
 */
final class ZinggNativeExpressionPlugin extends ExpressionPlugin {
  private val typeUrl = "type.googleapis.com/ai.zingg.native.connect.v1.ZinggNativeExpression"
  override def transform(payload: Array[Byte], planner: SparkConnectPlanner): Optional[Expression] = {
    if (payload == null || payload.isEmpty) Optional.empty()
    else throw new IllegalArgumentException("zingg-native Connect payload requires protocol v1 child-expression encoding")
  }
}
