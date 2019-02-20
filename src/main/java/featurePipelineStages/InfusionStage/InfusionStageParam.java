package featurePipelineStages.InfusionStage;

import org.apache.spark.ml.param.BooleanParam;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCols;

/**
 *
 */
public interface InfusionStageParam extends HasInputCols, HasOutputCols {
    String[] DEFAULT_INPUT_COLS = new String[]{"infusion_inputCol1", "infusion_inputCol2"};
    String[] DEFAULT_OUTPUT_COLS = new String[]{"infusion_outputCol1", "infusion_outputCol2"};

    //InputCols
    default StringArrayParam initInputCols() {
        StringArrayParam inputColsParam = new StringArrayParam(this, "inputCols", "The column which will be stacked up as one single column");
        setDefault(inputColsParam, DEFAULT_INPUT_COLS);
        return inputColsParam;
    }

    default void setInputCols(String[] inputCols) {
        set(inputCols(), inputCols);
    }

    @Override
    default String[] getInputCols() {
        return getOrDefault(inputCols());
    }

    //OutputCols
    default StringArrayParam initOutputCols() {
        StringArrayParam outputColsParam = new StringArrayParam(this, "outputCols", "Output the transformed columns");
        setDefault(outputColsParam, DEFAULT_OUTPUT_COLS);
        return outputColsParam;
    }

    default void setOutputCols(String[] outputCols) {
        set(outputCols(), outputCols);
    }

    @Override
    default String[] getOutputCols() {
        return getOrDefault(outputCols());
    }

    //TrainingFlag
    BooleanParam trainingFlag();

    /**
     * The pipeline fit method will create a stage model, we need to customize the behavior of infusion in training and testing.
     *
     * @return
     */
    default BooleanParam initTrainingFlag() {
        BooleanParam trainingFlag = new BooleanParam(this, "trainingFlag", "whether the pipeline is in training phase");
        setDefault(trainingFlag, true);
        return trainingFlag;
    }

    default void setTrainingFlag(boolean isTraining) {
        set(trainingFlag(), isTraining);
    }

    default boolean getTrainingFlag() {
        return (boolean) getOrDefault(trainingFlag());
    }

    //SourceIdCol
    Param<String> sourceIdCol();

    default Param<String> initSourceIdCol() {
        Param<String> sourceIdCol = new Param<>(this, "sourceIdCol", "col name for source artifact id");
        return sourceIdCol;
    }

    default String getSourceIdCol() {
        return getOrDefault(sourceIdCol());
    }

    default void setSourceIdCol(String sIdCol) {
        set(sourceIdCol(), sIdCol);
    }

    //TargetIdCol
    Param<String> targetIdCol();
    default Param<String> initTargetIdCol() {
        Param<String> targetIdCol = new Param<>(this, "targetIdCol", "col name for target artifact id");
        return targetIdCol;
    }

    default String getTargetIdCol() {
        return getOrDefault(targetIdCol());
    }

    default void setTargetIdCol(String tIdCol) {
        set(targetIdCol(), tIdCol);
    }

}
