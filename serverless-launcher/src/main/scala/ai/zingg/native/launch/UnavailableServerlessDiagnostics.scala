package ai.zingg.native.launch

import org.apache.spark.sql.SparkSession

/**
 * Fail-fast placeholders for diagnostic entry points whose implementations are
 * not part of the production Serverless launcher artifact yet.
 *
 * Keeping these flags explicit is preferable to making the whole launcher
 * uncompilable: ordinary Zingg execution remains usable, while callers that
 * request an unavailable diagnostic receive a deterministic error.
 */
private[launch] object ServerlessGraphBenchmark {
  def run(spark: SparkSession): Unit = {
    require(spark != null, "Spark session is required")
    throw new UnsupportedOperationException(
      "--native-graph-benchmark is not available in the production Serverless launcher")
  }
}

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
