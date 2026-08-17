package ai.zingg.native

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class RewriteArchitectureTest {
  @Test def parsesAllExecutionModes(): Unit = {
    assertEquals(NativeExecutionMode.STRICT, NativeExecutionMode.parse("strict"))
    assertThrows(classOf[IllegalArgumentException], () => NativeExecutionMode.parse("fallback"))
  }

  @Test def registryRejectsDuplicateSemanticRules(): Unit = {
    val rule = new RewriteRule {
      val id = "one"
      val operation = NativeOperation.ExactSimilarity
      def apply(left: org.apache.spark.sql.Column, right: Option[org.apache.spark.sql.Column], context: RewriteContext) = left
    }
    assertThrows(classOf[IllegalArgumentException], () => RewriteRegistry(Seq(rule, rule)))
  }

  @Test def strictGuardFailsClosed(): Unit = {
    val report = NativeCompatibilityAnalyzer.analyze("train", Seq((NativeOperation.JaroSimilarity, false, "callUDF")))
    val context = RewriteContext(null, NativeExecutionMode.STRICT, RuntimeDescriptor("4.1.0", "2.13"), "train")
    assertThrows(classOf[NativeRewriteUnsupportedException], () => NativePlanGuard.requireCompatible(report, context))
    assertTrue(report.unsupported.nonEmpty)
  }

  @Test def defaultRegistryContainsOnlyStablePublicRules(): Unit = {
    assertEquals(
      Set("similarity.exact", "similarity.jaccard", "similarity.jaro", "preprocess.trim", "preprocess.case_normalize",
        "blocking.first1Chars", "blocking.first2Chars", "blocking.first3Chars", "blocking.first4Chars",
        "blocking.last1Chars", "blocking.last2Chars", "blocking.last3Chars",
        "blocking.lastWord", "blocking.isNullOrEmpty", "blocking.identityString",
        "blocking.identityInteger", "blocking.identityLong", "blocking.lessThanZero"),
      NativeRewriteRegistry.default.operationIds.toSet)
  }
}
