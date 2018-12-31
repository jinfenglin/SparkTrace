package featurePipeline.NullRemoveWrapper;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.shared.HasOutputCol;

/**
 *
 */
public interface InnerStageImplementHasOutputCol extends HasOutputCol {
    String DEFAULT_OUTPUT_COL = "outputCol";

    default Param<String> initOutputCol() {
        Param<String> outputColParam = new Param<String>(this, "outputCols", "Output the transformed columns");
        setDefault(outputColParam, DEFAULT_OUTPUT_COL);
        return outputColParam;
    }

    <T extends PipelineStage> T setOutputCol(String colName);
}
