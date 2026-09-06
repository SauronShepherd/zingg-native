package ai.zingg.native

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class NativeOperationProviderTest {
  @Test def operationInventoryUsesStableIDs(): Unit = {
    assertEquals("similarity.exact", NativeOperation.resolve("similarity.exact").id)
    assertEquals("preprocess.case_normalize", NativeOperation.resolve("preprocess.case_normalize").id)
  }

  @Test def similarityAliasUsesConcreteClassSimpleName(): Unit = {
    assertEquals(
      "JaroWinklerFunction",
      NativeOperationProvider.similarityAlias("ai.zingg.similarity.JaroWinklerFunction"))
  }

  @Test def unsupportedSimilarityNamesTheExactClass(): Unit = {
    val className = "ai.zingg.similarity.UnknownSimilarity"
    val error = assertThrows(
      classOf[NativeRewriteUnsupportedException],
      () => NativeOperationProvider.similarityAlias(className))

    assertTrue(error.getMessage.contains(className))
    assertTrue(error.getMessage.contains("UnknownSimilarity"))
    assertFalse(error.getMessage.contains("[Ljava.lang.String;@"))
  }
}
