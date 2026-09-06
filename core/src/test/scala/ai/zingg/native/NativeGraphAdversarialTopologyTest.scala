package ai.zingg.native

import java.nio.file.Files
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NativeGraphAdversarialTopologyTest {
  @Test def connectedComponentsMatchesReferenceAcrossAdversarialTopologies(): Unit = {
    val materializeRoot = Files.createTempDirectory("zingg-native-graph-adversarial-").toUri.toString
    val previous = sys.props.get("zingg.native.graph.materializePath")
    sys.props.put("zingg.native.graph.materializePath", materializeRoot)
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("NativeGraphAdversarialTopologyTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      val cases = Seq(
        "long-chain" -> (
          (1L to 16L).toVector,
          (1L until 16L).map(value => value -> (value + 1L)).toVector),
        "wide-star" -> (
          (1L to 16L).toVector,
          (2L to 16L).map(value => 1L -> value).toVector),
        "duplicates-self-loops-and-isolate" -> (
          Vector(1L, 2L, 3L, 4L, 99L),
          Vector((1L, 1L), (1L, 2L), (1L, 2L), (2L, 2L), (2L, 3L), (3L, 3L), (3L, 4L), (4L, 4L))))

      cases.foreach { case (name, (vertices, edges)) =>
        val context = RewriteContext(
          spark,
          NativeExecutionMode.STRICT,
          RuntimeDescriptor(spark.version, "2.13"),
          "graph-convergence-test",
          s"adversarial-$name")

        val actual = NativeGraph.connectedComponents(
          vertices.toDF("left"),
          edges.toDF("left", "right"),
          "left",
          "right",
          "cluster",
          context,
          maxIterations = 32)
          .select("left", "cluster")
          .as[(Long, Long)]
          .collect()
          .toMap

        assertEquals(referenceComponents(vertices, edges), actual, s"adversarial topology $name")
      }
    } finally {
      spark.stop()
      previous match {
        case Some(value) => sys.props.put("zingg.native.graph.materializePath", value)
        case None => sys.props.remove("zingg.native.graph.materializePath")
      }
    }
  }

  private def referenceComponents(vertices: Seq[Long], edges: Seq[(Long, Long)]): Map[Long, Long] = {
    val parent = mutable.Map.from(vertices.map(value => value -> value))
    def find(value: Long): Long = {
      val current = parent(value)
      if (current == value) value
      else {
        val root = find(current)
        parent.update(value, root)
        root
      }
    }
    edges.foreach { case (left, right) =>
      val leftRoot = find(left)
      val rightRoot = find(right)
      if (leftRoot != rightRoot) parent.update(math.max(leftRoot, rightRoot), math.min(leftRoot, rightRoot))
    }
    vertices.map(value => value -> find(value)).toMap
  }
}
