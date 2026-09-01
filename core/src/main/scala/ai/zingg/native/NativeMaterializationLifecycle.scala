package ai.zingg.native

import java.util.UUID
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes

/** Owns only the short-lived UUID-scoped workspace used by native rewrites. */
object NativeMaterializationLifecycle {
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
    val normalized = Option(root).map(_.trim).getOrElse("")
    val candidate = Paths.get(normalized).toAbsolutePath.normalize()
    require(normalized.matches(".*[\\\\/].native-transient[\\\\/][0-9a-fA-F-]{36}$"),
      s"Refusing to clean non-run-scoped native materialization path: $normalized")
    require(candidate.getParent.getFileName.toString == ".native-transient",
      s"Refusing to clean non-run-scoped native materialization path: $normalized")
    UUID.fromString(candidate.getFileName.toString)
    rejectSymlinkAncestors(candidate)
    if (!Files.exists(candidate, java.nio.file.LinkOption.NOFOLLOW_LINKS)) false
    else {
      require(!Files.isSymbolicLink(candidate), s"Refusing to clean symbolic-link run root: $normalized")
      deleteTree(candidate)
    }
  }

  def exists(root: String): Boolean = {
    val normalized = Option(root).map(_.trim).getOrElse("")
    require(normalized.matches(".*\\/.native-transient\\/[0-9a-fA-F-]{36}$"),
      s"Refusing to inspect non-run-scoped native materialization path: $normalized")
    Files.exists(Paths.get(normalized))
  }

  private def deleteTree(path: Path): Boolean = {
    val canonicalRoot = path.toRealPath()
    Files.walkFileTree(canonicalRoot, new SimpleFileVisitor[Path] {
        private def verify(path: Path): Unit = {
          require(!Files.isSymbolicLink(path), s"Refusing to traverse symbolic link: $path")
          val canonical = path.toRealPath()
          require(canonical.startsWith(canonicalRoot), s"Refusing to delete path outside run root: $path")
        }

        override def preVisitDirectory(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
          verify(path)
          FileVisitResult.CONTINUE
        }

        override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
          verify(path)
          Files.delete(path)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(path: Path, error: java.io.IOException): FileVisitResult = {
          if (error != null) throw error
          verify(path)
          Files.delete(path)
          FileVisitResult.CONTINUE
        }
      })
    true
  }

  private def rejectSymlinkAncestors(path: Path): Unit = {
    var current: Path = path
    while (current != null) {
      require(!Files.isSymbolicLink(current), s"Refusing to clean path through symbolic link: $current")
      current = current.getParent
    }
  }
}
