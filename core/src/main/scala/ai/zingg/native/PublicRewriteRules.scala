package ai.zingg.native

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lower, trim}

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

  val all: Seq[RewriteRule] = Seq(Exact, Jaccard, Jaro, Trim, CaseNormalize)
}

object NativeRewriteRegistry {
  val default: RewriteRegistry = RewriteRegistry(PublicRewriteRules.all)
}
