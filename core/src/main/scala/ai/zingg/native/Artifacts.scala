package ai.zingg.native

/** Versioned references to the artifacts produced by a future native trainer.
  *
  * These are deliberately contracts, not a threshold-model implementation:
  * upstream Zingg persists both a learned model and a blocking tree. Keeping
  * the references transport-neutral lets Classic and Connect adopt the same
  * artifact format when the declarative trainer is implemented.
  */
final case class BlockingTreeArtifact(
    schemaVersion: Int,
    artifactPath: String,
    checksum: String
) {
  require(schemaVersion == 1, "unsupported blocking-tree artifact schema")
  require(artifactPath.nonEmpty, "blocking-tree artifact path must be non-empty")
  require(checksum.nonEmpty, "blocking-tree artifact checksum must be non-empty")
}

final case class ModelArtifact(
    schemaVersion: Int,
    modelType: String,
    artifactPath: String,
    checksum: String,
    featureColumns: Seq[String],
    positivePairs: Long,
    negativePairs: Long,
    blockingTree: BlockingTreeArtifact
) {
  require(schemaVersion == 1, "unsupported model artifact schema")
  require(modelType.nonEmpty, "model type must be non-empty")
  require(artifactPath.nonEmpty, "model artifact path must be non-empty")
  require(checksum.nonEmpty, "model artifact checksum must be non-empty")
  require(featureColumns.nonEmpty, "model artifact must declare feature columns")
  require(positivePairs >= 5, "training requires at least five positive pairs")
  require(negativePairs >= 5, "training requires at least five negative pairs")
}

final case class TrainingEvidence(positivePairs: Long, negativePairs: Long) {
  require(positivePairs >= 0, "positive pair count must not be negative")
  require(negativePairs >= 0, "negative pair count must not be negative")

  def isSufficient: Boolean = positivePairs >= 5 && negativePairs >= 5
}

object ArtifactSchema {
  val currentVersion: Int = 1

  /** Reject ambiguous paths before handing them to a Spark writer. */
  def validatePath(path: String): String = {
    require(path != null && path.nonEmpty, "artifact path must be non-empty")
    require(!path.exists(_.isControl), "artifact path must not contain control characters")
    val segments = path.replace('\\', '/').split('/').toSeq
    require(!segments.contains(".."), "artifact path must not contain parent traversal")
    path
  }
}
