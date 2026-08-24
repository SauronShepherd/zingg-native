package zingg.spark.client.util;

import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import zingg.common.client.ZFrame;
import zingg.common.client.ZinggClientException;
import zingg.common.client.pipe.FilePipe;
import zingg.common.client.pipe.Pipe;
import zingg.common.client.util.reader.IDFReader;
import zingg.common.client.util.reader.ReadStrategy;
import zingg.spark.client.SparkFrame;
import zingg.spark.client.util.reader.SparkReadStrategyFactory;

/** Public DataFrameReader boundary with eager native read validation. */
public class SparkDFReader implements IDFReader<Dataset<Row>, Row, Column> {
    protected final DataFrameReader reader;

    public SparkDFReader(SparkSession session, Pipe<Dataset<Row>, Row, Column> pipe) {
        reader = session.read();
        initializeReaderForPipe(pipe);
    }

    @Override public IDFReader<Dataset<Row>, Row, Column> getReader() { return this; }
    @Override public IDFReader<Dataset<Row>, Row, Column> format(String value) { reader.format(value); return this; }
    @Override public IDFReader<Dataset<Row>, Row, Column> option(String key, String value) { reader.option(key, value); return this; }
    @Override public IDFReader<Dataset<Row>, Row, Column> setSchema(String schema) { reader.schema(StructType.fromDDL(schema)); return this; }

    @Override
    public ZFrame<Dataset<Row>, Row, Column> load() {
        Dataset<Row> loaded = reader.load();
        // Preserve Spark's lazy-read contract.  Optional Zingg inputs are
        // handled by the ordinary reader/phase orchestration; adding an
        // action here changes failure timing and can duplicate work.
        return new SparkFrame(loaded);
    }

    @Override
    public ZFrame<Dataset<Row>, Row, Column> read(Pipe<Dataset<Row>, Row, Column> pipe)
            throws ZinggClientException, Exception {
        ReadStrategy<Dataset<Row>, Row, Column> strategy = getReadStrategy(pipe);
        return strategy.read(this, pipe);
    }

    protected ReadStrategy<Dataset<Row>, Row, Column> getReadStrategy(Pipe<Dataset<Row>, Row, Column> pipe) {
        return new SparkReadStrategyFactory().getStrategy(pipe);
    }

    protected void initializeReaderForPipe(Pipe<Dataset<Row>, Row, Column> pipe) {
        format(pipe.getFormat());
        if (pipe.getSchema() != null) setSchema(pipe.getSchema());
        for (Map.Entry<String, String> entry : pipe.getProps().entrySet()) {
            option(FilePipe.LOCATION.equals(entry.getKey()) ? FilePipe.PATH : entry.getKey(), entry.getValue());
        }
        option("mode", "PERMISSIVE");
    }

    public DataFrameReader getDataFrameReader() { return reader; }
}
