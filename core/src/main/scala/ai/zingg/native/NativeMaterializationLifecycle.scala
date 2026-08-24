package ai.zingg.native

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

/** Owns only the short-lived UUID-scoped workspace used by native rewrites. */
object NativeMaterializationLifecycle {
  private val cleaned = new AtomicBoolean(false)

  def runRoot(zinggDir: String, runId: String): String = {
    val safeRunId = Option(runId).map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException("native run ID must be non-empty"))
    UUID.fromString(safeRunId)
    val root = Option(zinggDir).map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException("native transient cleanup requires --zinggDir"))
    s"${root.stripSuffix("/")}/.native-transient/$safeRunId"
  }

  /** Delete a validated run root; repeated cleanup calls are harmless. */
  def cleanup(root: String): Boolean = synchronized {
    if (cleaned.getAndSet(true)) return false
    val normalized = Option(root).map(_.trim).getOrElse("")
    require(normalized.matches(".*\\/.native-transient\\/[0-9a-fA-F-]{36}$"),
      s"Refusing to clean non-run-scoped native materialization path: $normalized")
    deleteTree(Paths.get(normalized))
  }

  def exists(root: String): Boolean = {
    val normalized = Option(root).map(_.trim).getOrElse("")
    require(normalized.matches(".*\\/.native-transient\\/[0-9a-fA-F-]{36}$"),
      s"Refusing to inspect non-run-scoped native materialization path: $normalized")
    Files.exists(Paths.get(normalized))
  }

  private def deleteTree(path: Path): Boolean = {
    if (!Files.exists(path)) false
    else {
      if (Files.isDirectory(path))
        {
          val children = Files.list(path)
          try children.iterator().asScala.foreach(deleteTree)
          finally children.close()
        }
      Files.deleteIfExists(path)
    }
  }
}
