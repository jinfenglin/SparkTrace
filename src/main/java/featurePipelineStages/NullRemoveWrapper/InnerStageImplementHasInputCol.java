package featurePipelineStages.NullRemoveWrapper;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.shared.HasInputCol;

import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 *
 */
public interface InnerStageImplementHasInputCol extends HasInputCol, HasInnerStage {
    String DEFAULT_INPUT_COL = "inputCol";

    default Param<String> initInputCol() {
        Param<String> inputColParam = new Param<>(this, "inputCol", "An input column synchronized with inner stage's input column");
        setDefault(inputColParam, DEFAULT_INPUT_COL);
        return inputColParam;
    }

    default void setInnerInputCol(String colName) {
        HasInputCol hasInputCol = (HasInputCol) getInnerStage();
        hasInputCol.set(hasInputCol.inputCol(), colName);
        setInnerStage((PipelineStage) hasInputCol);
    }

    default String getInnerInputCol() {
        HasInputCol hasInputCol = (HasInputCol) getInnerStage();
        return hasInputCol.getInputCol();
    }

    default void setInputCol(String colName) {
        set(inputCol(), colName);
        setInnerInputCol(colName);
    }

    @Override
    default String getInputCol() {
        return getOrDefault(inputCol());
    }

    @Override
    default void org$apache$spark$ml$param$shared$HasInputCol$_setter_$inputCol_$eq(Param param) {
    }

    /**
     * Assign the inner stage's inputCol to out side class thus make the status of inner and outer synchronized.
     */
    default void syncInputCol() {
        try {
            setInputCol(getInnerInputCol());
        } catch (NoSuchElementException e) {
            setInputCol(null);
            Logger.getLogger(this.getClass().getName()).warning(String.format("No default value for stage %s inputCol, its NullRemover's IO params are set to null", getInnerStage().uid()));
        }
    }
}