package zingg.spark.core.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.collection.JavaConverters;
import zingg.common.client.ZFrame;
import zingg.common.client.util.ColName;
import zingg.common.core.util.GraphUtil;
import zingg.spark.client.SparkFrame;
import ai.zingg.nativebridge.NativeOperationProvider;

/**
 * Zingg graph integration with a public-DataFrame native path.
 *
 * Native mode has no compile-time or runtime dependency on GraphFrames.  The
 * original Zingg 0.7 GraphFrames implementation remains available only for
 * OFF/AUDIT compatibility and is loaded reflectively if GraphFrames is present.
 */
public class SparkGraphUtil implements GraphUtil<Dataset<Row>, Row, Column> {

    private Dataset<Row> legacyFrame(Dataset<Row> frame) {
        // Cache/persist is deliberately absent from the patched production
        // graph path. Serverless cannot use DataFrame cache APIs, and graph
        // correctness does not depend on the legacy performance hint.
        return frame;
    }

    public ZFrame<Dataset<Row>, Row, Column> buildGraph(
            ZFrame<Dataset<Row>, Row, Column> vOrig,
            ZFrame<Dataset<Row>, Row, Column> ed) {
        // Keep the original external-id rename contract: GraphFrames used "id"
        // internally and ordinary user data can already contain that name.
        Dataset<Row> vertices = vOrig.df();
        Dataset<Row> edges = ed.df();
        vertices = vertices.withColumnRenamed(ColName.ID_EXTERNAL_ORIG_COL, ColName.ID_EXTERNAL_COL);

        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(
                vertices.sparkSession(), "graph.connectedComponents");
        if (nativeProvider.shouldRewrite()) {
            Dataset<Row> returnGraph = nativeProvider.connectedComponents(
                    vertices,
                    edges,
                    ColName.ID_COL,
                    ColName.COL_PREFIX + ColName.ID_COL,
                    ColName.CLUSTER_COLUMN,
                    128);
            returnGraph = returnGraph.withColumnRenamed(
                    ColName.ID_EXTERNAL_COL, ColName.ID_EXTERNAL_ORIG_COL);
            return new SparkFrame(returnGraph);
        }
        nativeProvider.auditLegacyOperation("graph.connectedComponents", "GraphFrames.connectedComponents", getClass().getName());

        // Caching belongs exclusively to the legacy GraphFrames path. Databricks
        // Serverless rejects DataFrame cache/persist APIs, so native execution
        // must never construct these calls.
        vertices = legacyFrame(vertices);

        // Legacy Zingg path.  Reflection is deliberate: GraphFrames must never
        // become a compile/runtime dependency of the Serverless native artifact.
        Dataset<Row> v1 = vertices.withColumnRenamed(ColName.ID_COL, "id");
        Dataset<Row> v = legacyFrame(v1.select("id"));
        List<Column> cols = new ArrayList<Column>();
        cols.add(edges.col(ColName.ID_COL));
        cols.add(edges.col(ColName.COL_PREFIX + ColName.ID_COL));

        Dataset<Row> e = edges.select(JavaConverters.asScalaIteratorConverter(
                cols.iterator()).asScala().toSeq());
        e = legacyFrame(e.toDF("src", "dst"));

        Dataset<Row> returnGraph = legacyFrame(runLegacyGraphFramesConnectedComponents(v, e));
        returnGraph = returnGraph.join(
                vertices,
                returnGraph.col("id").equalTo(vertices.col(ColName.ID_COL)));
        returnGraph = returnGraph.drop(ColName.ID_COL).withColumnRenamed("id", ColName.ID_COL);
        returnGraph = returnGraph.withColumnRenamed("component", ColName.CLUSTER_COLUMN);
        returnGraph = returnGraph.withColumnRenamed(
                ColName.ID_EXTERNAL_COL, ColName.ID_EXTERNAL_ORIG_COL);
        return new SparkFrame(returnGraph);
    }

    @SuppressWarnings("unchecked")
    private Dataset<Row> runLegacyGraphFramesConnectedComponents(
            Dataset<Row> vertices,
            Dataset<Row> edges) {
        try {
            Class<?> graphFrameClass = Class.forName("org.graphframes.GraphFrame");
            Constructor<?> constructor = graphFrameClass.getConstructor(Dataset.class, Dataset.class);
            Object graphFrame = constructor.newInstance(vertices, edges);
            Method connectedComponents = graphFrameClass.getMethod("connectedComponents");
            Object builder = connectedComponents.invoke(graphFrame);
            Method run = builder.getClass().getMethod("run");
            return (Dataset<Row>) run.invoke(builder);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "GraphFrames is required only when ZINGG_NATIVE_MODE is OFF/AUDIT. " +
                    "Use REWRITE/STRICT for the GraphFrames-free native implementation, " +
                    "or install the GraphFrames dependency for legacy execution.", e);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(
                    "Unable to invoke the legacy Zingg 0.7 GraphFrames connected-components API", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException(
                    "Legacy GraphFrames connected-components execution failed", cause);
        }
    }
}
