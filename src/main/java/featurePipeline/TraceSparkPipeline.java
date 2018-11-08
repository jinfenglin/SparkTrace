package featurePipeline;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A warpper for spark pipeline. Include pipeline and pipeline model at the same time
 */
public abstract class TraceSparkPipeline {
    protected Pipeline pipeline;
    protected PipelineModel model;

    public TraceSparkPipeline() {
        pipeline = new Pipeline();
    }

    public void fit(Dataset trainingData) {
        if (pipeline.getStages().length == 0) {
            return;
        }
        model = pipeline.fit(trainingData);
    }

    public Dataset<Row> apply(Dataset rawData) {
        if (model == null) {
            return rawData;
        }
        return model.transform(rawData);
    }

    public void setPipelineStages(PipelineStage[] stages) {
        pipeline.setStages(stages);
    }
}
