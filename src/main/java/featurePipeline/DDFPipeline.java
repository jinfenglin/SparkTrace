package featurePipeline;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DDFPipeline {
    Pipeline ddfPipeline;
    PipelineModel ddfPipelineModel;

    public void fit(Dataset<Row> trainingData) {
        ddfPipelineModel = ddfPipeline.fit(trainingData);
    }

    public Dataset<Row> apply(Dataset<Row> rawData) {
        return ddfPipelineModel.transform(rawData);
    }
}
