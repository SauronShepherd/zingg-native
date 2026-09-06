package ai.zingg.native.launch

import ai.zingg.native.NativeRewriteUnsupportedException
import ai.zingg.nativebridge.NativeOperationProvider
import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** Bounded Serverless graph contract probe for nontrivial components. */
object ServerlessGraphProbe {
  def run(spark: SparkSession): Unit = {
    val vertexSchema = StructType(Seq(StructField("id", DataTypes.StringType, false)))
    val edgeSchema = StructType(Seq(
      StructField("id", DataTypes.StringType, false),
      StructField("right_id", DataTypes.StringType, false)))
    val vertices = new ArrayList[Row]()
    ('a' to 'm').foreach(id => vertices.add(RowFactory.create(id.toString)))
    val edges = new ArrayList[Row]()
    Seq(("a", "b"), ("b", "a"), ("a", "b"), ("b", "c"), ("c", "c"),
      ("d", "e"), ("f", "g"), ("g", "h"), ("h", "i"), ("i", "f"),
      ("j", "k"), ("j", "l"), ("j", "m"), ("m", "m")).foreach { case (left, right) =>
      edges.add(RowFactory.create(left, right))
    }
    val vertexFrame = spark.createDataFrame(vertices, vertexSchema)
    val edgeFrame = spark.createDataFrame(edges, edgeSchema)
    val provider = NativeOperationProvider.fromSpark(spark, "graph-probe")

    var rejected = false
    try {
      provider.connectedComponents(vertexFrame, edgeFrame, "id", "right_id", "cluster", 1).collect()
    } catch {
      case _: NativeRewriteUnsupportedException => rejected = true
    }
    require(rejected, "STRICT graph probe must reject an insufficient iteration bound")

    val previousMode = sys.props.get("zingg.native.mode")
    var rewriteRejected = false
    try {
      System.setProperty("zingg.native.mode", "REWRITE")
      val rewriteProvider = NativeOperationProvider.fromSpark(spark, "graph-probe-rewrite")
      rewriteProvider.connectedComponents(vertexFrame, edgeFrame, "id", "right_id", "cluster", 1).collect()
    } catch {
      case _: NativeRewriteUnsupportedException => rewriteRejected = true
    } finally {
      previousMode match {
        case Some(value) => System.setProperty("zingg.native.mode", value)
        case None => System.clearProperty("zingg.native.mode")
      }
    }
    require(rewriteRejected, "REWRITE graph probe must reject an insufficient iteration bound")

    val previousLimit = sys.props.get("zingg.native.graph.maxIterations")
    var propertyRejected = false
    try {
      System.setProperty("zingg.native.graph.maxIterations", "1")
      provider.connectedComponents(vertexFrame, edgeFrame, "id", "right_id", "cluster", 8).collect()
    } catch {
      case _: NativeRewriteUnsupportedException => propertyRejected = true
    } finally {
      previousLimit match {
        case Some(value) => System.setProperty("zingg.native.graph.maxIterations", value)
        case None => System.clearProperty("zingg.native.graph.maxIterations")
      }
    }
    require(propertyRejected, "STRICT graph probe must honor the launcher graph iteration property")

    val result = provider.connectedComponents(vertexFrame, edgeFrame, "id", "right_id", "cluster", 8)
      .select("id", "cluster").collect()
    val groups = result.groupBy(_.getString(1)).values.map(_.map(_.getString(0)).toSet).toSet
    val expected = Set(Set("a", "b", "c"), Set("d", "e"), Set("f", "g", "h", "i"),
      Set("j", "k", "l", "m"))
    require(groups == expected, s"Graph component mismatch: actual=$groups expected=$expected")
    println("NATIVE_GRAPH_PROBE_PASS cases=isolated,single-edge,chain,cycle,star,duplicate,reverse,self-edge iterationFailure=true rewriteIterationFailure=true propertyIterationFailure=true")
  }
}
