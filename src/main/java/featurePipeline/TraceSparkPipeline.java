package featurePipeline;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A warpper for spark pipeline. Include pipeline and pipeline pipelineModel at the same time
 */
public abstract class TraceSparkPipeline {
    protected Pipeline pipeline;
    protected PipelineModel pipelineModel;

    public TraceSparkPipeline() {
        pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{});
    }

    public PipelineModel fit(Dataset trainingData) {
        if (pipeline.getStages().length == 0) {
            return null;
        }
        pipelineModel = pipeline.fit(trainingData);
        return pipelineModel;
    }

    public Dataset<Row> apply(Dataset rawData) {
        if (pipelineModel == null) {
            return rawData;
        }
        return pipelineModel.transform(rawData);
    }

    public void setPipelineStages(PipelineStage[] stages) {
        pipeline.setStages(stages);
    }
}
