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

  @Test def buildsCatalystJaccardExpression(): Unit = {
    val expression = CatalystSimilarity("JACCARD_SIMILARITY", org.apache.spark.sql.catalyst.expressions.Literal("a"), org.apache.spark.sql.catalyst.expressions.Literal("b"))
    assertTrue(expression.isInstanceOf[org.apache.spark.sql.catalyst.expressions.If])
  }

  @Test def gatewayAdvertisesOnlyVerifiedClassicPhases(): Unit = {
    val phases = new gateway.ClassicGateway().supportedPhases.toSet
    assertEquals(Set("preprocess", "findTrainingData", "buildTrainingPairs", "label", "updateLabel"), phases)
  }

  @Test def gatewayExposesArtifactSchemaVersions(): Unit = {
    val g = new gateway.ClassicGateway()
    assertEquals(1, g.modelArtifactSchemaVersion)
    assertEquals(1, g.blockingTreeArtifactSchemaVersion)
    assertTrue(g.capabilityMetadata.contains("model-artifact-schema-v1"))
    assertTrue(g.capabilityMetadata.contains("blocking-tree-artifact-schema-v1"))
  }
}
