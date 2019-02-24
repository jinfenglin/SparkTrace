package featurePipelineStages.LDAWithIO;

import org.apache.spark.ml.clustering.LDAParams;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasOutputCol;

/**
 *
 */
public interface LDAIOParam extends HasInputCol, HasOutputCol, LDAParams {
    String DEFAULT_INPUT_COL = "input_col";
    String DEFAULT_OUTPUT_COL = "output_col";

    default Param<String> initInputCol() {
        Param<String> inputColParam = new Param<>(this, "inputCol", "Token input for LDA");
        setDefault(inputColParam, DEFAULT_INPUT_COL);
        return inputColParam;
    }

    default Param<String> initOutputCol() {
        Param<String> outputColParam = new Param<>(this, "outputCol", "Output the topic distribution for documents");
        setDefault(outputColParam, DEFAULT_OUTPUT_COL);
        return outputColParam;
    }

    @Override
    default void org$apache$spark$ml$param$shared$HasInputCol$_setter_$inputCol_$eq(Param param) {
    }

    @Override
    default Param<String> inputCol() {
        return featuresCol();
    }

    @Override
    default String getInputCol() {
        return getOrDefault(inputCol());
    }

    @Override
    default void org$apache$spark$ml$param$shared$HasOutputCol$_setter_$outputCol_$eq(Param param) {

    }

    @Override
    default Param<String> outputCol() {
        return topicDistributionCol();
    }

    @Override
    default String getOutputCol() {
        return getOrDefault(outputCol());
    }


}
