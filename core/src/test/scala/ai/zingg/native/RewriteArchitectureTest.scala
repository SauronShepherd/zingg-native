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
      val operation = NativeOperation.resolve("similarity.exact")
      def apply(left: org.apache.spark.sql.Column, right: Option[org.apache.spark.sql.Column], context: RewriteContext) = left
    }
    assertThrows(classOf[IllegalArgumentException], () => RewriteRegistry(Seq(rule, rule)))
  }

  @Test def strictGuardFailsClosed(): Unit = {
    val report = NativeCompatibilityAnalyzer.analyze("train", Seq((NativeOperation.resolve("similarity.jaro"), false, "callUDF")))
    val context = RewriteContext(null, NativeExecutionMode.STRICT, RuntimeDescriptor("4.1.0", "2.13"), "train")
    assertThrows(classOf[NativeRewriteUnsupportedException], () => NativePlanGuard.requireCompatible(report, context))
    assertTrue(report.unsupported.nonEmpty)
  }

  @Test def disabledRuleFailsClosedWithOperationAndRule(): Unit = {
    val context = RewriteContext(null, NativeExecutionMode.STRICT,
      RuntimeDescriptor("4.1.0", "2.13"), "similarity", "run-test",
      Map("disabledRules" -> "similarity.SimilarityFunctionExact"))
    val failure = assertThrows(classOf[NativeRewriteUnsupportedException], () =>
      Core.rewrite(null, "similarity.SimilarityFunctionExact", "left", None, "out", context))
    assertTrue(failure.getMessage.contains("similarity.SimilarityFunctionExact"))
  }

  @Test def defaultRegistryContainsOnlyStablePublicRules(): Unit = {
    val ids = NativeRewriteRegistry.default.operationIds
    assertTrue(ids.nonEmpty)
    assertTrue(ids.contains("preprocess.trim"))
    assertTrue(ids.contains("blocking.first1Chars"))
    assertTrue(ids.contains("similarity.SimilarityFunctionExact"))
    assertTrue(ids.forall(id => !id.contains("Catalyst") && !id.contains("SparkContext")))
  }
}
