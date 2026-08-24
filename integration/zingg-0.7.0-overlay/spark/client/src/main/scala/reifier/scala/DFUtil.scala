package zingg.scala

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import zingg.common.client.util.ColName

/** Public DataFrame equivalents for the row-number choke points in Zingg 0.7.0. */
object DFUtil {
  private def prepend(df: Dataset[Row], name: String): Dataset[Row] = {
    // Managed Spark Connect does not safely support schema inspection or
    // wildcard re-projection at this boundary. withColumn is one public
    // expression, replaces an existing generated name on re-entry, and keeps
    // the required invariant: unique named IDs and equivalent cluster
    // membership. Positional column order is not part of this product
    // contract.
    df.withColumn(name, monotonically_increasing_id())
  }

  def addRowNumber(df: Dataset[Row], spark: SparkSession): Dataset[Row] =
    prepend(df, ColName.ID_COL)

  def addClusterRowNumber(df: Dataset[Row], spark: SparkSession): Dataset[Row] =
    prepend(df, ColName.CLUSTER_COLUMN)
}
