package ai.zingg.native

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}

class CoreRegistryTest {
  @Test def exposesStableCertifiedOperations(): Unit = {
    val ids = NativeRewriteRegistry.default.operationIds.toSet
    assertTrue(ids.contains("similarity.SimilarityFunctionExact"))
    assertTrue(ids.contains("similarity.JaroWinklerFunction"))
    assertTrue(ids.contains("similarity.AffineGapSimilarityFunction"))
  }

  @Test def gatewayAdvertisesOnlyVerifiedClassicPhases(): Unit = {
    assertEquals(0, new gateway.ClassicGateway().supportedPhases.length)
  }
}
