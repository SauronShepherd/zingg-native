package ai.zingg.native.connect

import org.apache.spark.sql.catalyst.expressions.{ArrayIntersect, ArrayUnion, Cast, EqualTo, Expression, If, IsNull, Literal, Lower, RegExpExtractAll, Size, Divide, Or, StringTrim}
import org.apache.spark.sql.types.{DoubleType, StringType}

/** Spark-internal Connect boundary; deliberately excluded from the common core JAR. */
object CatalystSimilarity {
  def apply(operationId: String, left: Expression, right: Expression): Expression = operationId match {
    case "EXACT_SIMILARITY" =>
      val one = Literal(1.0)
      If(Or(IsNull(left), IsNull(right)), one, If(EqualTo(left, right), one, Literal(0.0)))
    case "JACCARD_SIMILARITY" =>
      val l = RegExpExtractAll(Lower(Cast(left, StringType)), Literal("[\\p{L}\\p{N}]+"), Literal(0))
      val r = RegExpExtractAll(Lower(Cast(right, StringType)), Literal("[\\p{L}\\p{N}]+"), Literal(0))
      val empty = Or(IsNull(left), Or(IsNull(right), Or(EqualTo(left, Literal("")), EqualTo(right, Literal("")))))
      If(empty, Literal(1.0), Divide(Cast(Size(ArrayIntersect(l, r)), DoubleType), Cast(Size(ArrayUnion(l, r)), DoubleType)))
    case other => throw new IllegalArgumentException(s"Catalyst Connect operation not implemented: $other")
  }
}

object CatalystPreprocess {
  def apply(operationId: String, input: Expression): Expression = operationId match {
    case "CASE_NORMALIZE" => Lower(Cast(input, StringType))
    case "TRIM" => StringTrim(input)
    case other => throw new IllegalArgumentException(s"Catalyst Connect preprocessing not implemented: $other")
  }
}
