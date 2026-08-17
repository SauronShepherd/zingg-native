package ai.zingg.native

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{floor, length, lower, pow, regexp_extract, round, substring, trim, when}

/** Public Spark-expression rewrite rules shared by all supported runtimes. */
object PublicRewriteRules {
  abstract class SimilarityRule(
      val id: String,
      val operation: NativeOperation,
      similarity: NativeSimilarity) extends RewriteRule {
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column =
      similarity(left, right.getOrElse(throw new IllegalArgumentException(s"$id requires two operands")),
        NativeContext(context.spark, NativeMode.SAFE, context.runtime))
  }

  object Exact extends SimilarityRule("rewrite.similarity.exact", NativeOperation.ExactSimilarity, ExactSimilarity)
  object Jaccard extends SimilarityRule("rewrite.similarity.jaccard", NativeOperation.JaccardSimilarity, JaccardSimilarity)
  object Jaro extends SimilarityRule("rewrite.similarity.jaro", NativeOperation.JaroSimilarity, JaroSimilarity)

  object Trim extends RewriteRule {
    val id = "rewrite.preprocess.trim"
    val operation = NativeOperation.Trim
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column = trim(left)
  }

  object CaseNormalize extends RewriteRule {
    val id = "rewrite.preprocess.case_normalize"
    val operation = NativeOperation.CaseNormalize
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column = lower(left.cast("string"))
  }

  abstract class PrefixRule(val id: String, val operation: NativeOperation, width: Int) extends RewriteRule {
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column =
      when(left.isNull, left).otherwise(substring(left.cast("string"), 1, width))
  }
  object First1Chars extends PrefixRule("rewrite.blocking.first1Chars", NativeOperation.First1Chars, 1)
  object First2Chars extends PrefixRule("rewrite.blocking.first2Chars", NativeOperation.First2Chars, 2)
  object First3Chars extends PrefixRule("rewrite.blocking.first3Chars", NativeOperation.First3Chars, 3)
  object First4Chars extends PrefixRule("rewrite.blocking.first4Chars", NativeOperation.First4Chars, 4)

  abstract class SuffixRule(val id: String, val operation: NativeOperation, width: Int) extends RewriteRule {
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column =
      when(left.isNull, left).otherwise(substring(left.cast("string"), -width, width))
  }
  object Last1Chars extends SuffixRule("rewrite.blocking.last1Chars", NativeOperation.Last1Chars, 1)
  object Last2Chars extends SuffixRule("rewrite.blocking.last2Chars", NativeOperation.Last2Chars, 2)
  object Last3Chars extends SuffixRule("rewrite.blocking.last3Chars", NativeOperation.Last3Chars, 3)

  object LastWord extends RewriteRule {
    val id = "rewrite.blocking.lastWord"
    val operation = NativeOperation.LastWord
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column =
      when(left.isNull || length(left.cast("string")) === 0, left)
        .otherwise(regexp_extract(left.cast("string"), "([^ ]+)$", 1))
  }

  object IsNullOrEmpty extends RewriteRule {
    val id = "rewrite.blocking.isNullOrEmpty"
    val operation = NativeOperation.IsNullOrEmpty
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column =
      left.isNull || length(left.cast("string")) === 0
  }

  abstract class IdentityRule(val id: String, val operation: NativeOperation) extends RewriteRule {
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column = left
  }
  object IdentityString extends IdentityRule("rewrite.blocking.identityString", NativeOperation.IdentityString)
  object IdentityInteger extends IdentityRule("rewrite.blocking.identityInteger", NativeOperation.IdentityInteger)
  object IdentityLong extends IdentityRule("rewrite.blocking.identityLong", NativeOperation.IdentityLong)

  object LessThanZero extends RewriteRule {
    val id = "rewrite.blocking.lessThanZero"
    val operation = NativeOperation.LessThanZero
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column = left < 0
  }

  object Round extends RewriteRule {
    val id = "rewrite.blocking.round"
    val operation = NativeOperation.Round
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column = round(left)
  }
  abstract class TruncateDoubleRule(val id: String, val operation: NativeOperation, places: Int) extends RewriteRule {
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column = {
      val scale = pow(org.apache.spark.sql.functions.lit(10), org.apache.spark.sql.functions.lit(places))
      when(left.isNull, left).otherwise(floor(left * scale) / scale)
    }
  }
  object TruncateDouble1 extends TruncateDoubleRule("rewrite.blocking.truncateDoubleTo1Places", NativeOperation.TruncateDouble1, 1)
  object TruncateDouble2 extends TruncateDoubleRule("rewrite.blocking.truncateDoubleTo2Places", NativeOperation.TruncateDouble2, 2)
  object TruncateDouble3 extends TruncateDoubleRule("rewrite.blocking.truncateDoubleTo3Places", NativeOperation.TruncateDouble3, 3)
  abstract class TrimIntRule(val id: String, val operation: NativeOperation, digits: Int) extends RewriteRule {
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column = left / pow(org.apache.spark.sql.functions.lit(10), org.apache.spark.sql.functions.lit(digits))
  }
  object TrimInt1 extends TrimIntRule("rewrite.blocking.trimLast1DigitsInt", NativeOperation.TrimLastDigitsInt1, 1)
  object TrimInt2 extends TrimIntRule("rewrite.blocking.trimLast2DigitsInt", NativeOperation.TrimLastDigitsInt2, 2)
  object TrimInt3 extends TrimIntRule("rewrite.blocking.trimLast3DigitsInt", NativeOperation.TrimLastDigitsInt3, 3)

  abstract class RangeRule(val id: String, val operation: NativeOperation, lower: Int, upper: Int) extends RewriteRule {
    def apply(left: Column, right: Option[Column], context: RewriteContext): Column =
      when(left.isNotNull && left >= lower && left < upper, org.apache.spark.sql.functions.lit(1)).otherwise(org.apache.spark.sql.functions.lit(0))
  }
  object RangeDbl0To10 extends RangeRule("rewrite.blocking.rangeBetween0And10Dbl", NativeOperation.RangeDbl0To10, 0, 10)
  object RangeDbl10To100 extends RangeRule("rewrite.blocking.rangeBetween10And100Dbl", NativeOperation.RangeDbl10To100, 10, 100)
  object RangeDbl100To1000 extends RangeRule("rewrite.blocking.rangeBetween100And1000Dbl", NativeOperation.RangeDbl100To1000, 100, 1000)
  object RangeDbl1000To10000 extends RangeRule("rewrite.blocking.rangeBetween1000And10000Dbl", NativeOperation.RangeDbl1000To10000, 1000, 10000)
  object RangeInt0To10 extends RangeRule("rewrite.blocking.rangeBetween0And10Int", NativeOperation.RangeInt0To10, 0, 10)
  object RangeInt10To100 extends RangeRule("rewrite.blocking.rangeBetween10And100Int", NativeOperation.RangeInt10To100, 10, 100)
  object RangeInt100To1000 extends RangeRule("rewrite.blocking.rangeBetween100And1000Int", NativeOperation.RangeInt100To1000, 100, 1000)
  object RangeInt1000To10000 extends RangeRule("rewrite.blocking.rangeBetween1000And10000Int", NativeOperation.RangeInt1000To10000, 1000, 10000)

  val all: Seq[RewriteRule] = Seq(Exact, Jaccard, Jaro, Trim, CaseNormalize,
    First1Chars, First2Chars, First3Chars, First4Chars, Last1Chars, Last2Chars, Last3Chars,
    LastWord, IsNullOrEmpty, IdentityString, IdentityInteger, IdentityLong, LessThanZero, Round,
    TruncateDouble1, TruncateDouble2, TruncateDouble3, TrimInt1, TrimInt2, TrimInt3,
    RangeDbl0To10, RangeDbl10To100, RangeDbl100To1000, RangeDbl1000To10000,
    RangeInt0To10, RangeInt10To100, RangeInt100To1000, RangeInt1000To10000)
}

object NativeRewriteRegistry {
  val default: RewriteRegistry = RewriteRegistry(PublicRewriteRules.all)
}
