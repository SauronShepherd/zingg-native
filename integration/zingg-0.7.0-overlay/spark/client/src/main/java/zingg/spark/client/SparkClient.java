package zingg.spark.client;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import zingg.common.client.Client;
import zingg.common.client.ClientOptions;
import zingg.common.client.IZingg;
import zingg.common.client.ZinggClientException;
import zingg.common.client.arguments.model.IZArgs;
import zingg.common.client.util.PipeUtilBase;
import zingg.common.core.util.Analytics;
import zingg.common.core.util.Metric;
import zingg.spark.client.util.SparkPipeUtil;

/** Zingg 0.7.0 Spark client with a public Spark-only session boundary. */
public class SparkClient extends Client<SparkSession, Dataset<Row>, Row, Column, DataType> {
    private static final long serialVersionUID = 1L;
    protected static final String zFactoryClassName = "zingg.spark.core.executor.SparkZFactory";

    public SparkClient(IZArgs args, ClientOptions options) throws ZinggClientException { this(args, options, zFactoryClassName); }
    public SparkClient(IZArgs args, ClientOptions options, SparkSession s) throws ZinggClientException {
        this(args, options, s, zFactoryClassName);
    }
    public SparkClient() { this(zFactoryClassName); }
    public SparkClient(IZArgs args, ClientOptions options, String factory) throws ZinggClientException {
        super(args, options, factory);
    }
    public SparkClient(IZArgs args, ClientOptions options, SparkSession s, String factory) throws ZinggClientException {
        super(args, options, s, factory);
        Analytics.track(Metric.IS_PYTHON, "true", args.getCollectMetrics());
    }
    public SparkClient(String factory) { super(factory); }

    @Override
    public Client<SparkSession, Dataset<Row>, Row, Column, DataType> getClient(
            IZArgs args, ClientOptions options) throws ZinggClientException {
        return session == null ? new SparkClient(args, options) : new SparkClient(args, options, session);
    }

    public static void main(String... args) {
        try {
            new SparkClient().mainMethod(args);
        } catch (RuntimeException | Error failure) {
            // Databricks JAR-task output otherwise truncates Zingg's caught
            // exception to its message, which makes native evidence unusable.
            failure.printStackTrace(System.err);
            throw failure;
        }
    }

    @Override
    public SparkSession getSession() {
        if (session == null) {
            // The Serverless launcher installs the managed Connect session as
            // active. Reuse it; constructing a second session can detach the
            // ordinary Zingg client from the managed execution channel.
            scala.Option<SparkSession> activeOption = SparkSession.getActiveSession();
            SparkSession active = activeOption.isDefined() ? activeOption.get() : null;
            if (active == null && Boolean.parseBoolean(System.getProperty("zingg.native.managed", "false"))) {
                throw new IllegalStateException(
                        "Managed Zingg invocation has no active SparkSession; refusing to create a detached session");
            }
            setSession(active != null ? active : SparkSession.builder().appName("Zingg").getOrCreate());
        }
        return session;
    }

    @Override
    public PipeUtilBase<SparkSession, Dataset<Row>, Row, Column> getPipeUtil() {
        if (pipeUtil == null) setPipeUtil(new SparkPipeUtil(getSession()));
        return pipeUtil;
    }
}
