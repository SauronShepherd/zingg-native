package ai.zingg.native.connect

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.{EqualTo, If, IsNull, Literal, Or}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.ExpressionPlugin
import org.apache.spark.connect.proto.{Expression => ConnectExpression}
import org.sparkproject.connect.protobuf.CodedInputStream
import java.util.Optional

/**
 * Server-side extension entry point. The payload is parsed with Spark's shaded
 * protobuf runtime, then child expressions are delegated to Spark's planner.
 * Similarity semantics are built from Catalyst expressions, not UDFs.
 */
final class ZinggNativeExpressionPlugin extends ExpressionPlugin {
  override def transform(payload: Array[Byte], planner: SparkConnectPlanner): Optional[Expression] = {
    if (payload == null || payload.isEmpty) return Optional.empty()
    val in = CodedInputStream.newInstance(payload)
    var version = 0
    var operation = ""
    val args = scala.collection.mutable.ArrayBuffer.empty[ConnectExpression]
    var done = false
    while (!done) {
      in.readTag() match {
        case 0 => done = true
        case 8 => version = in.readUInt32()
        case 18 => operation = in.readString()
        case 26 => args += ConnectExpression.parseFrom(in.readByteArray())
        case 34 => in.readByteArray() // reserved options; validated by the caller
        case tag => in.skipField(tag)
      }
    }
    if (version != 1) throw new IllegalArgumentException(s"Unsupported zingg-native Connect protocol version: $version")
    if (args.size != 2) throw new IllegalArgumentException(s"$operation requires exactly two child expressions")
    val left = planner.transformExpression(args(0))
    val right = planner.transformExpression(args(1))
    operation match {
      case "EXACT_SIMILARITY" =>
        val one = Literal(1.0)
        val zero = Literal(0.0)
        Optional.of[Expression](If(Or(IsNull(left), IsNull(right)), one, If(EqualTo(left, right), one, zero)))
      case other => throw new IllegalArgumentException(s"Unknown zingg-native Connect operation: $other")
    }
  }
}
