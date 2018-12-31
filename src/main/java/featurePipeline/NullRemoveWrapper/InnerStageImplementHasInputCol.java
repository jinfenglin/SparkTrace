package featurePipeline.NullRemoveWrapper;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.shared.HasInputCol;

/**
 *
 */
public interface InnerStageImplementHasInputCol extends HasInputCol {
    String DEFAULT_INPUT_COL = "inputCol";

    default Param<String> initInputCol() {
        Param<String> inputColParam = new Param<>(this, "inputCols", "The column which will be stacked up as one single column");
        setDefault(inputColParam, DEFAULT_INPUT_COL);
        return inputColParam;
    }

    <T extends PipelineStage> T setInputCol(String colName);
}
