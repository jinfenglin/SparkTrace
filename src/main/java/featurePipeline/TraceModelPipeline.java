package featurePipeline;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TraceModelPipeline {
    private PipelineModel traceModel;
    private Pipeline traceModelPipeline;

    public TraceModelPipeline() {

    }

    public void fit(Dataset<Row> trainingData) {
        traceModel = traceModelPipeline.fit(trainingData);
    }

    public Dataset<Row> apply(Dataset<Row> rawData) {
        return traceModel.transform(rawData);
    }
}
