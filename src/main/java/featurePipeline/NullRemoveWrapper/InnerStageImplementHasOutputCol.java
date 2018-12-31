package featurePipeline.NullRemoveWrapper;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.shared.HasOutputCol;

import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * Synchronize the inner stage with the outside wrapping class.
 */
public interface InnerStageImplementHasOutputCol extends HasOutputCol, HasInnerStage {
    String DEFAULT_OUTPUT_COL = "outputCol";

    default Param<String> initOutputCol() {
        Param<String> outputColParam = new Param<>(this, "outputCol", "A output column synchronized with inner stage's output column");
        setDefault(outputColParam, DEFAULT_OUTPUT_COL);
        return outputColParam;
    }

    default void setInnerOutputCol(String colName) {
        HasOutputCol hasOutputCol = (HasOutputCol) getInnerStage();
        hasOutputCol.set(hasOutputCol.outputCol(), colName);
        setInnerStage((PipelineStage) hasOutputCol);
    }

    default String getInnerOutputCol() {
        HasOutputCol hasOutputCol = (HasOutputCol) getInnerStage();
        return hasOutputCol.getOutputCol();
    }

    default void setOutputCol(String colName) {
        set(outputCol(), colName);
        setInnerOutputCol(colName);
    }

    @Override
    default void org$apache$spark$ml$param$shared$HasOutputCol$_setter_$outputCol_$eq(Param param) {
    }

    @Override
    default String getOutputCol() {
        return getOrDefault(outputCol());
    }

    default void syncOutputCol() {
        try {
            setOutputCol(getInnerOutputCol());
        } catch (NoSuchElementException e) {
            setOutputCol(null);
            Logger.getLogger(this.getClass().getName()).warning(String.format("No default value for stage %s outputCol, its NullRemover's IO params are set to null", getInnerStage().uid()));
        }
    }
}