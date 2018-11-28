package featurePipeline;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.Params;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;

/**
 * Process a pair of document/artifacts in the dataset and generate one or more columns of features.
 * DDF is not skippable yet, but can be modified to be skippable. Experiment this feature on the SDF first.
 */
public abstract class DDFStage extends Estimator {
    public DDFStage(String columnName1, String columnName2) {
    }

}
