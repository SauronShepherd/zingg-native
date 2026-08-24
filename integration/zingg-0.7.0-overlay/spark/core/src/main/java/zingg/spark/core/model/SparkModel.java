package zingg.spark.core.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.functions;

import ai.zingg.nativebridge.NativeModelHandle;
import ai.zingg.nativebridge.NativeOperationProvider;
import zingg.common.client.FieldDefinition;
import zingg.common.client.ZFrame;
import zingg.common.client.util.ColName;
import zingg.common.core.feature.Feature;
import zingg.common.core.model.Model;
import zingg.common.core.similarity.function.SimFunction;
import zingg.spark.client.SparkFrame;

/**
 * Zingg 0.7 SparkModel with a transparent public-DataFrame native execution
 * path.  Zingg still owns feature definitions and phase orchestration; only the
 * Spark-ML training/prediction boundary is replaced in REWRITE/STRICT mode.
 */
public class SparkModel extends Model<SparkSession, Dataset<Row>, Row, Column, DataType>{

    public static final Log LOG = LogFactory.getLog(SparkModel.class);
    List<NativeFeatureCreator> featureCreators;
    List<String> nativeFeatureColumns;
    NativeModelHandle nativeModel;

    public SparkModel(SparkSession s, Map<FieldDefinition, Feature<DataType>> f) {
        super(s);
        featureCreators = new ArrayList<NativeFeatureCreator>();
        nativeFeatureColumns = new ArrayList<String>();
        int count = 0;
        for (FieldDefinition fd : f.keySet()) {
            Feature fea = f.get(fd);
            List<SimFunction> sfList = fea.getSimFunctions();
            for (SimFunction sf : sfList) {
                String outputCol = getColumnName(fd.fieldName, sf.getName(), count);
                columnsAdded.add(outputCol);
                nativeFeatureColumns.add(outputCol);
                NativeFeatureCreator st = new NativeFeatureCreator(
                        fd.fieldName, sf.getClass().getName(), outputCol);
                count++;
                featureCreators.add(st);
            }
        }

        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(s, "model.initialization");
        if (!nativeProvider.shouldRewrite()) {
            initializeLegacySparkMlPipeline();
        } else {
            // Preserve upstream temporary-column bookkeeping even though native
            // prediction builds these expressions directly.
            columnsAdded.add(ColName.FEATURE_VECTOR_COL);
            columnsAdded.add(ColName.FEATURE_COL);
            columnsAdded.add(ColName.PROBABILITY_COL);
            columnsAdded.add(ColName.RAW_PREDICTION);
        }
    }

    private void initializeLegacySparkMlPipeline() {
        throw new IllegalStateException("Legacy Spark ML pipeline is unavailable in the Serverless artifact");
    }

    @Override
    public void fit(ZFrame<Dataset<Row>,Row,Column> pos, ZFrame<Dataset<Row>,Row,Column> neg) {
        fitCore(pos, neg);
    }

    public ZFrame<Dataset<Row>,Row,Column> transformTrainingData(
            ZFrame<Dataset<Row>,Row,Column> pos,
            ZFrame<Dataset<Row>,Row,Column> neg) {
        // Do not serialize training onto one partition. Serverless training
        // must retain the upstream partitioning and let Spark schedule the
        // public DataFrame plan across the available workers.
        ZFrame<Dataset<Row>,Row,Column> union = pos.union(neg);
        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(session, "model.trainingData");
        if (nativeProvider.shouldRewrite()) {
            // cache/persist/checkpoint are unsupported on Databricks Serverless.
            return union;
        }
        nativeProvider.auditLegacyOperation("model.trainingData.cache", "DataFrame.cache", getClass().getName());
        return union;
    }

    public ZFrame<Dataset<Row>,Row,Column> applyFitPipeline(ZFrame<Dataset<Row>,Row,Column> input) {
        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(session, "model.train");
        if (nativeProvider.shouldRewrite()) {
            Dataset<Row> fitInput = input.df();
            String materializeRoot = System.getProperty("zingg.native.model.materializePath");
            if (materializeRoot != null && !materializeRoot.trim().isEmpty()) {
                String path = materializeRoot.replaceAll("/+$", "") + "/ordinary-input-" + UUID.randomUUID();
                // The similarity provider has already materialized every
                // scalar feature into a narrow Parquet boundary. Reassembling
                // those columns through independently generated monotonic IDs
                // and repeated joins creates a second large Connect plan and
                // can stall before NativeModel.fit is reached. Write one
                // scalar feature frame from that stable boundary instead; it
                // contains no polynomial expansion and preserves ordinary
                // Zingg feature ordering.
                List<Column> projection = new ArrayList<Column>();
                projection.add(input.df().col(ColName.MATCH_FLAG_COL));
                for (String feature : nativeFeatureColumns) {
                    projection.add(input.df().col(feature));
                }
                String assembledPath = path + "/assembled";
                input.df().select(projection.toArray(new Column[projection.size()]))
                        .write().mode("overwrite").parquet(assembledPath);
                fitInput = session.read().parquet(assembledPath);
                System.setProperty("zingg.native.model.inputPath", assembledPath);
            }
            try {
                nativeModel = nativeProvider.fitModel(
                    fitInput,
                    nativeFeatureColumns.toArray(new String[nativeFeatureColumns.size()]),
                    ColName.MATCH_FLAG_COL);
            } finally {
                if (materializeRoot != null && !materializeRoot.trim().isEmpty()) {
                    System.clearProperty("zingg.native.model.inputPath");
                }
            }
            return input;
        }
        throw new IllegalStateException("Legacy Spark ML training is unavailable in the Serverless artifact");
        /*
        nativeProvider.auditLegacyOperation(
                "model.sparkMlPipeline",
                "VectorAssembler/PolynomialExpansion/LogisticRegression/CrossValidator",
                getClass().getName());

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(pipelineStage.toArray(new PipelineStage[pipelineStage.size()]));
        LOG.debug("Pipeline is " + pipeline);
        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(lr.regParam(), getGrid(0.0001, 1, 10, true))
                .addGrid(lr.threshold(), getGrid(0.40, 0.55, 0.05, false))
                .build();
        binaryClassificationEvaluator = new BinaryClassificationEvaluator();
        binaryClassificationEvaluator.setLabelCol(ColName.MATCH_FLAG_COL);
        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(binaryClassificationEvaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(2);
        CrossValidatorModel cvModel = cv.fit(input.df());
        transformer = cvModel;
        LOG.debug("threshold after fitting is " + lr.getThreshold());
        return input;
        */
    }

    public ZFrame<Dataset<Row>,Row,Column> fitCore(
            ZFrame<Dataset<Row>,Row,Column> pos,
            ZFrame<Dataset<Row>,Row,Column> neg) {
        ZFrame<Dataset<Row>,Row,Column> input = transform(transformTrainingData(pos, neg));
        return applyFitPipeline(input);
    }

    public void load(String path) {
        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(session, "model.load");
        if (nativeProvider.shouldRewrite()) {
            nativeModel = nativeProvider.loadModel(path);
            return;
        }
        throw new IllegalStateException("Legacy Spark ML model loading is unavailable in the Serverless artifact");
        /*
        nativeProvider.auditLegacyOperation("model.crossValidator.load", "CrossValidatorModel.load", getClass().getName());
        transformer = CrossValidatorModel.load(path);
        */
    }

    public ZFrame<Dataset<Row>,Row,Column> predict(ZFrame<Dataset<Row>,Row,Column> data) {
        return predict(data, true);
    }

    @Override
    public ZFrame<Dataset<Row>,Row,Column> predict(ZFrame<Dataset<Row>,Row,Column> data, boolean isDrop) {
        return dropFeatureCols(predictCore(data), isDrop);
    }

    @Override
    public ZFrame<Dataset<Row>,Row,Column> predictCore(ZFrame<Dataset<Row>,Row,Column> data) {
        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(session, "model.predict");
        return transformAndPredict(transform(data));
    }

    public ZFrame<Dataset<Row>,Row,Column> transformAndPredict(ZFrame<Dataset<Row>,Row,Column> data) {
        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(session, "model.predict");
        Dataset<Row> predictWithFeatures;
        if (nativeProvider.shouldRewrite()) {
            if (nativeModel == null) {
                throw new IllegalStateException(
                        "Native Zingg model is not fitted or loaded. Train the model or load a zingg-native model artifact before prediction.");
            }
            predictWithFeatures = nativeProvider.predictModel(
                    data.df(), nativeModel,
                    ColName.FEATURE_VECTOR_COL,
                    ColName.FEATURE_COL,
                    ColName.PROBABILITY_COL,
                    ColName.RAW_PREDICTION,
                    ColName.PREDICTION_COL,
                    ColName.SCORE_COL);
        } else {
            throw new IllegalStateException("Legacy Spark ML prediction is unavailable in the Serverless artifact");
            /*
            nativeProvider.auditLegacyOperation("model.crossValidator.predict", "CrossValidatorModel.transform", getClass().getName());
            predictWithFeatures = transformer.transform(data.df());
            predictWithFeatures = vve.transform(predictWithFeatures);
            */
        }
        LOG.debug("Return schema is " + predictWithFeatures.schema());
        return new SparkFrame(predictWithFeatures);
    }

    public void save(String path) throws IOException{
        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(session, "model.save");
        if (nativeProvider.shouldRewrite()) {
            if (nativeModel == null) {
                throw new IllegalStateException("Native Zingg model has not been fitted; nothing can be saved");
            }
            nativeProvider.saveModel(nativeModel, path);
            return;
        }
        throw new IllegalStateException("Legacy Spark ML model persistence is unavailable in the Serverless artifact");
        /*
        nativeProvider.auditLegacyOperation("model.crossValidator.save", "CrossValidatorModel.write", getClass().getName());
        ((CrossValidatorModel) transformer).write().overwrite().save(path);
        */
    }

    public ZFrame<Dataset<Row>,Row,Column> transform(Dataset<Row> input) {
        NativeOperationProvider nativeProvider = NativeOperationProvider.fromSpark(input.sparkSession(), "similarity.batch");
        if (nativeProvider.shouldRewrite() && !featureCreators.isEmpty()) {
            String[] semanticClasses = new String[featureCreators.size()];
            String[] leftColumns = new String[featureCreators.size()];
            String[] rightColumns = new String[featureCreators.size()];
            String[] outputColumns = new String[featureCreators.size()];
            for (int index = 0; index < featureCreators.size(); index++) {
                NativeFeatureCreator creator = featureCreators.get(index);
                semanticClasses[index] = creator.semanticClassName;
                leftColumns[index] = creator.inputColumn;
                rightColumns[index] = ColName.COL_PREFIX + creator.inputColumn;
                outputColumns[index] = creator.outputColumn;
            }
            return new SparkFrame(nativeProvider.similarityBatchByZinggName(
                    input, semanticClasses, leftColumns, rightColumns, outputColumns));
        }
        for (NativeFeatureCreator bsf: featureCreators) {
            input = bsf.transform(input);
        }
        return new SparkFrame(input);
    }

    public ZFrame<Dataset<Row>,Row,Column> transform(ZFrame<Dataset<Row>,Row,Column> i) {
        return transform(i.df());
    }

    public List<NativeFeatureCreator> getFeatureCreators() {
        return featureCreators;
    }

    /** Native-only feature boundary; deliberately not a Spark ML Transformer. */
    private static final class NativeFeatureCreator {
        private final String inputColumn;
        private final String semanticClassName;
        private final String outputColumn;

        private NativeFeatureCreator(String inputColumn, String semanticClassName, String outputColumn) {
            this.inputColumn = inputColumn;
            this.semanticClassName = semanticClassName;
            this.outputColumn = outputColumn;
        }

        private Dataset<Row> transform(Dataset<Row> input) {
            NativeOperationProvider provider = NativeOperationProvider.fromSpark(input.sparkSession(), "similarity");
            if (!provider.shouldRewrite()) {
                throw new IllegalStateException("Legacy Spark ML/UDF similarity is unavailable in the Serverless artifact");
            }
            return provider.similarityByZinggName(input, semanticClassName, inputColumn,
                    ColName.COL_PREFIX + inputColumn, outputColumn);
        }
    }

    @Override
    public void register() {
        if (featureCreators != null) {
            // Native feature creators build public Column/DataFrame expressions
            // at transform time; there is no UDF registration boundary.
        }
    }
}
