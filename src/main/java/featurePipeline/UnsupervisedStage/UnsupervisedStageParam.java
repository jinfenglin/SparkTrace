package featurePipeline.UnsupervisedStage;

import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCols;

/**
 *
 */
public interface UnsupervisedStageParam extends HasInputCols, HasOutputCols {
    String[] DEFAULT_INPUT_COLS = new String[]{"unsupervised_inputCol1", "unsupervised_inputCol2"};
    String[] DEFAULT_OUTPUT_COLS = new String[]{"unsupervised_outputCol1", "unsupervised_outputCol2"};

    default StringArrayParam initInputCols() {
        StringArrayParam inputColsParam = new StringArrayParam(this, "inputCols", "The column which will be stacked up as one single column");
        setDefault(inputColsParam, DEFAULT_INPUT_COLS);
        return inputColsParam;
    }

    @Override
    default String[] getInputCols() {
        return getOrDefault(inputCols());
    }

    default void setInputCols(String[] inputCols) {
        set(inputCols(), inputCols);
    }

    default StringArrayParam initOutputCols() {
        StringArrayParam outputColsParam = new StringArrayParam(this, "outputCols", "Output the transformed columns");
        setDefault(outputColsParam, DEFAULT_OUTPUT_COLS);
        return outputColsParam;
    }

    @Override
    default String[] getOutputCols() {
        return getOrDefault(outputCols());
    }

    default void setOutputCols(String[] outputCols) {
        set(outputCols(), outputCols);
    }


    default void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {

    }

    default void org$apache$spark$ml$param$shared$HasOutputCols$_setter_$outputCols_$eq(StringArrayParam stringArrayParam) {

    }

}
