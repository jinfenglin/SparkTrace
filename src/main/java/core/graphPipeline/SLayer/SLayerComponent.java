package core.graphPipeline.SLayer;

import org.apache.spark.ml.Pipeline;

public interface SLayerComponent {
    Pipeline toPipeline() throws Exception;
}
