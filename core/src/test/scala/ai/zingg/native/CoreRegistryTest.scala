package ai.zingg.native

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}

class CoreRegistryTest {
  @Test def exposesStableCertifiedOperations(): Unit = {
    val ids = SimilarityRegistry.metadata.map(_.id).toSet
    assertEquals(Set("EXACT_SIMILARITY", "JACCARD_SIMILARITY", "JARO_SIMILARITY"), ids)
    assertEquals("JARO_SIMILARITY", SimilarityRegistry.resolve("JARO_SIMILARITY", NativeMode.SAFE).id)
  }

  @Test def rejectsUnknownOperations(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => SimilarityRegistry.resolve("NOPE", NativeMode.SAFE))
  }
}
