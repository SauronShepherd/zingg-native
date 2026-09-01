package ai.zingg.native

import java.nio.file.Files
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class NativeMaterializationLifecycleTest {
  @Test def cleansDifferentRunRootsInOneJvm(): Unit = {
    val base = Files.createTempDirectory("zingg-native-lifecycle-")
    val first = NativeMaterializationLifecycle.runRoot(base.toString, "11111111-1111-4111-8111-111111111111")
    val second = NativeMaterializationLifecycle.runRoot(base.toString, "22222222-2222-4222-8222-222222222222")
    Files.createDirectories(java.nio.file.Paths.get(first))
    Files.createDirectories(java.nio.file.Paths.get(second))
    Files.write(java.nio.file.Paths.get(first, "sentinel"), Array[Byte](1))
    Files.write(java.nio.file.Paths.get(second, "sentinel"), Array[Byte](2))

    assertTrue(NativeMaterializationLifecycle.cleanup(first))
    assertTrue(NativeMaterializationLifecycle.cleanup(second))
    assertFalse(Files.exists(java.nio.file.Paths.get(first)))
    assertFalse(Files.exists(java.nio.file.Paths.get(second)))
  }

  @Test def rejectsSymlinkRunRootWithoutTouchingTarget(): Unit = {
    val base = Files.createTempDirectory("zingg-native-lifecycle-")
    val target = Files.createDirectories(base.resolve("outside"))
    Files.write(target.resolve("sentinel"), Array[Byte](1))
    val runParent = Files.createDirectories(base.resolve(".native-transient"))
    val link = runParent.resolve("33333333-3333-4333-8333-333333333333")
    try {
      Files.createSymbolicLink(link, target)
      assertThrows(classOf[IllegalArgumentException], () => NativeMaterializationLifecycle.cleanup(link.toString))
      assertTrue(Files.exists(target.resolve("sentinel")))
    } finally {
      Files.deleteIfExists(link)
      Files.deleteIfExists(target.resolve("sentinel"))
      Files.deleteIfExists(target)
      Files.deleteIfExists(runParent)
      Files.deleteIfExists(base)
    }
  }

  @Test def rejectsSymlinkDescendantWithoutTouchingTarget(): Unit = {
    val base = Files.createTempDirectory("zingg-native-lifecycle-")
    val root = base.resolve(".native-transient/44444444-4444-4444-8444-444444444444")
    val target = Files.createDirectories(base.resolve("outside"))
    Files.createDirectories(root)
    Files.write(target.resolve("sentinel"), Array[Byte](1))
    val link = root.resolve("linked-directory")
    try {
      Files.createSymbolicLink(link, target)
      assertThrows(classOf[IllegalArgumentException], () => NativeMaterializationLifecycle.cleanup(root.toString))
      assertTrue(Files.exists(target.resolve("sentinel")))
      Files.delete(link)
      assertTrue(NativeMaterializationLifecycle.cleanup(root.toString))
      assertFalse(Files.exists(root))
    } finally {
      Files.deleteIfExists(link)
      Files.deleteIfExists(target.resolve("sentinel"))
      Files.deleteIfExists(target)
      Files.deleteIfExists(root)
      Files.deleteIfExists(root.getParent)
      Files.deleteIfExists(base)
    }
  }
}
