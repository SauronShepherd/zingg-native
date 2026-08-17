package ai.zingg.native

import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ArtifactContractTest {
  private val tree = BlockingTreeArtifact(1, "dbfs:/blocking-tree", "sha256:tree")

  @Test
  def acceptsVersionedArtifactsWithUpstreamMinimums(): Unit = {
    val artifact = ModelArtifact(
      1,
      "UPSTREAM_MODEL",
      "dbfs:/model",
      "sha256:model",
      Seq("name", "city"),
      positivePairs = 5,
      negativePairs = 5,
      tree
    )
    assert(artifact.schemaVersion == ArtifactSchema.currentVersion)
  }

  @Test
  def rejectsInsufficientTrainingEvidence(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () =>
      ModelArtifact(1, "UPSTREAM_MODEL", "dbfs:/model", "sha256:model", Seq("name"), 4, 5, tree)
    )
    assertThrows(classOf[IllegalArgumentException], () =>
      ModelArtifact(1, "UPSTREAM_MODEL", "dbfs:/model", "sha256:model", Seq("name"), 5, 4, tree)
    )
  }

  @Test
  def rejectsUnsafeArtifactPaths(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => ArtifactSchema.validatePath("dbfs:/out/../escape"))
    assertThrows(classOf[IllegalArgumentException], () => ArtifactSchema.validatePath("dbfs:/out/\nnext"))
    assert(ArtifactSchema.validatePath("/Volumes/catalog/schema/volume/run") != null)
    assert(ArtifactSchema.validatePath("s3://bucket/prefix") == "s3://bucket/prefix")
  }

  @Test
  def reportsTrainingEvidenceSufficiency(): Unit = {
    assert(!TrainingEvidence(4, 5).isSufficient)
    assert(!TrainingEvidence(5, 4).isSufficient)
    assert(TrainingEvidence(5, 5).isSufficient)
  }
}
