package ai.zingg.native.launch

import ai.zingg.native.NativeDiagnostics
import ai.zingg.nativebridge.NativeOperationProvider
import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * Bounded, deterministic connected-components benchmark for the production
 * Serverless launcher. The benchmark emits measurements but intentionally has
 * no wall-clock pass/fail threshold; correctness is the gate and timings are
 * evidence for future regression baselines.
 */
private[launch] object ServerlessGraphBenchmark {
  private final case class Scenario(name: String, vertexCount: Int, edges: Seq[(String, String)]) {
    val maxIterations: Int = vertexCount + 4
  }

  private val VertexSchema = StructType(Seq(StructField("id", DataTypes.StringType, false)))
  private val EdgeSchema = StructType(Seq(
    StructField("id", DataTypes.StringType, false),
    StructField("right_id", DataTypes.StringType, false)))

  def run(spark: SparkSession): Unit = {
    require(spark != null, "Spark session is required")
    val provider = NativeOperationProvider.fromSpark(spark, "graph-benchmark")
    val scenarios = Seq(16, 64).flatMap { size =>
      Seq(chain(size), star(size))
    }

    var totalElapsedMs = 0L
    scenarios.foreach { scenario =>
      val vertices = new ArrayList[Row]()
      (0 until scenario.vertexCount).foreach(index => vertices.add(RowFactory.create(vertexId(index))))
      val edges = new ArrayList[Row]()
      scenario.edges.foreach { case (left, right) => edges.add(RowFactory.create(left, right)) }

      val vertexFrame = spark.createDataFrame(vertices, VertexSchema)
      val edgeFrame = spark.createDataFrame(edges, EdgeSchema)
      val started = System.nanoTime()
      val (result, iterationEvidence) = NativeDiagnostics.captureGraphIterations {
        provider
          .connectedComponents(vertexFrame, edgeFrame, "id", "right_id", "cluster", scenario.maxIterations)
          .select("id", "cluster")
          .collect()
      }
      val elapsedMs = (System.nanoTime() - started) / 1000000L
      totalElapsedMs += elapsedMs
      val finalIteration = iterationEvidence.lastOption
      val iterations = finalIteration.map(_.iteration).getOrElse(0)
      val converged = finalIteration.exists(_.frontierEmpty)

      require(result.length == scenario.vertexCount,
        s"Graph benchmark ${scenario.name} lost vertices: ${result.length}/${scenario.vertexCount}")
      val components = result.iterator.map(_.getString(1)).toSet.size
      require(components == 1,
        s"Graph benchmark ${scenario.name} expected one component, observed $components")
      require(iterationEvidence.nonEmpty,
        s"Graph benchmark ${scenario.name} emitted no convergence evidence")
      require(converged,
        s"Graph benchmark ${scenario.name} did not report convergence")

      println(
        s"NATIVE_GRAPH_BENCHMARK_CASE topology=${scenario.name} " +
          s"vertices=${scenario.vertexCount} edges=${scenario.edges.size} " +
          s"maxIterations=${scenario.maxIterations} iterations=$iterations " +
          s"converged=$converged components=$components elapsedMs=$elapsedMs")
      println(
        s"NATIVE_GRAPH_BENCHMARK_JSON {\"kind\":\"case\",\"schemaVersion\":1," +
          s"\"topology\":\"${scenario.name}\",\"vertices\":${scenario.vertexCount}," +
          s"\"edges\":${scenario.edges.size},\"maxIterations\":${scenario.maxIterations}," +
          s"\"iterations\":$iterations,\"converged\":$converged," +
          s"\"components\":$components,\"elapsedMs\":$elapsedMs}")
    }

    val maxVertices = scenarios.map(_.vertexCount).max
    println(
      s"NATIVE_GRAPH_BENCHMARK_PASS cases=${scenarios.size} maxVertices=$maxVertices " +
        s"totalElapsedMs=$totalElapsedMs thresholds=none")
    println(
      s"NATIVE_GRAPH_BENCHMARK_JSON {\"kind\":\"summary\",\"schemaVersion\":1," +
        s"\"cases\":${scenarios.size},\"maxVertices\":$maxVertices," +
        s"\"totalElapsedMs\":$totalElapsedMs,\"thresholds\":null}")
  }

  private def chain(size: Int): Scenario =
    Scenario("chain", size, (0 until size - 1).map(index => vertexId(index) -> vertexId(index + 1)))

  private def star(size: Int): Scenario =
    Scenario("star", size, (1 until size).map(index => vertexId(0) -> vertexId(index)))

  private def vertexId(index: Int): String = f"v$index%04d"
}

/**
 * Fail-fast placeholder for the LDBC diagnostic entry point whose production
 * implementation remains a separate iteration.
 */
private[launch] object LdbcGraphStress {
  final case class Config(
      dataset: String,
      vertices: String,
      edges: String,
      expected: Option[String])

  def run(spark: SparkSession, config: Config, output: String): Unit = {
    require(spark != null, "Spark session is required")
    require(config != null, "LDBC stress configuration is required")
    require(output != null, "LDBC stress output is required")
    throw new UnsupportedOperationException(
      "--native-ldbc-stress is not available in the production Serverless launcher")
  }
}
