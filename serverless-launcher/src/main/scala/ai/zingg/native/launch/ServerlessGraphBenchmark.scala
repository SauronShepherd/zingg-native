package ai.zingg.native.launch

import org.apache.spark.sql.SparkSession

/**
 * Lightweight managed-runtime benchmark entry point.
 *
 * Keep benchmark wiring in production compile scope because the Serverless
 * launcher exposes the flag from the production artifact. The actual graph
 * correctness probe remains the authoritative workload so benchmark mode
 * cannot bypass the same fail-closed graph contract.
 */
object ServerlessGraphBenchmark {
  def run(spark: SparkSession): Unit = {
    val started = System.nanoTime()
    ServerlessGraphProbe.run(spark)
    val elapsedMillis = (System.nanoTime() - started) / 1000000L
    System.err.println(s"SERVERLESS_GRAPH_BENCHMARK elapsedMillis=$elapsedMillis correctness=pass")
  }
}
