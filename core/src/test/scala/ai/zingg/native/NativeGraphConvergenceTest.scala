package ai.zingg.native

import java.nio.file.Files
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class NativeGraphConvergenceTest {
  @Test def equalAggregateLabelsDoNotImplyConvergence(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("NativeGraphConvergenceTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      val left = Seq((1L, 1L, 1L), (2L, 4L, 1L)).toDF("src", "min_nbr", "cnt")
      val collision = Seq((1L, 2L, 1L), (2L, 3L, 1L)).toDF("src", "min_nbr", "cnt")
      val same = Seq((2L, 4L, 9L), (1L, 1L, 7L)).toDF("src", "min_nbr", "cnt")

      assertTrue(left.selectExpr("sum(min_nbr)").head().getLong(0) == collision.selectExpr("sum(min_nbr)").head().getLong(0))
      assertFalse(NativeGraph.sameAssignments(left, collision))
      assertTrue(NativeGraph.sameAssignments(left, same))
    } finally {
      spark.stop()
    }
  }

  @Test def connectedComponentsHonorsIterationBudgetBeforeFalseConvergence(): Unit = {
    val materializeRoot = Files.createTempDirectory("zingg-native-graph-").toUri.toString
    val previous = sys.props.get("zingg.native.graph.materializePath")
    sys.props.put("zingg.native.graph.materializePath", materializeRoot)
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("NativeGraphConvergenceBudgetTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      val vertices = Seq(1L, 2L, 3L, 4L).toDF("left")
      val edges = Seq((1L, 2L), (2L, 3L), (3L, 4L)).toDF("left", "right")
      val context = RewriteContext(
        spark,
        NativeExecutionMode.STRICT,
        RuntimeDescriptor(spark.version, "2.13"),
        "graph-convergence-test",
        "collision-budget-test")

      val error = assertThrows(classOf[NativeRewriteUnsupportedException], () =>
        NativeGraph.connectedComponents(
          vertices,
          edges,
          "left",
          "right",
          "cluster",
          context,
          maxIterations = 1))
      assertTrue(error.getMessage.contains("did not converge within 1 iterations"))
    } finally {
      spark.stop()
      previous match {
        case Some(value) => sys.props.put("zingg.native.graph.materializePath", value)
        case None => sys.props.remove("zingg.native.graph.materializePath")
      }
    }
  }

  @Test def connectedComponentsProducesStableGoldenAssignments(): Unit = {
    val materializeRoot = Files.createTempDirectory("zingg-native-graph-golden-").toUri.toString
    val previous = sys.props.get("zingg.native.graph.materializePath")
    sys.props.put("zingg.native.graph.materializePath", materializeRoot)
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("NativeGraphConvergenceGoldenTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      import spark.implicits._
      val vertices = Seq(1L, 2L, 3L, 4L, 10L, 11L, 99L).toDF("left")
      val edges = Seq((1L, 2L), (2L, 3L), (3L, 4L), (10L, 11L)).toDF("left", "right")
      val context = RewriteContext(
        spark,
        NativeExecutionMode.STRICT,
        RuntimeDescriptor(spark.version, "2.13"),
        "graph-convergence-test",
        "golden-assignment-test")

      val assignments = NativeGraph.connectedComponents(
        vertices,
        edges,
        "left",
        "right",
        "cluster",
        context,
        maxIterations = 10)
        .select("left", "cluster")
        .as[(Long, Long)]
        .collect()
        .toSet

      assertEquals(
        Set((1L, 1L), (2L, 1L), (3L, 1L), (4L, 1L), (10L, 10L), (11L, 10L), (99L, 99L)),
        assignments)
    } finally {
      spark.stop()
      previous match {
        case Some(value) => sys.props.put("zingg.native.graph.materializePath", value)
        case None => sys.props.remove("zingg.native.graph.materializePath")
      }
    }
  }
}
