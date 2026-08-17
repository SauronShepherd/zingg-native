package ai.zingg.native

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{length, lower, substring, trim, when}

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

  val all: Seq[RewriteRule] = Seq(Exact, Jaccard, Jaro, Trim, CaseNormalize,
    First1Chars, First2Chars, First3Chars, First4Chars, Last1Chars, Last2Chars, Last3Chars)
}

object NativeRewriteRegistry {
  val default: RewriteRegistry = RewriteRegistry(PublicRewriteRules.all)
}
