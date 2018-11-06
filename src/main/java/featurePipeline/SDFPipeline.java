package featurePipeline;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import traceability.components.abstractComponents.TraceArtifact;

/**
 * Single Document Feature pipeline. The stages in this pipeline process one column of the dataset and generate one or
 * more columns of data.
 */
public class SDFPipeline<T extends TraceArtifact> {
    private PipelineModel sdfModel;
    private Pipeline pipeline;

    public SDFPipeline() {

    }

    public void fit(Dataset<T> trainingData) {
        sdfModel = pipeline.fit(trainingData);
    }

    public Dataset<Row> apply(Dataset<T> rawData) {
        return sdfModel.transform(rawData);
    }
}
