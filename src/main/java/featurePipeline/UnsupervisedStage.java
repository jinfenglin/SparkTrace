package featurePipeline;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;

/**
 * A wrapper for any pipeline stage. This stage collect multiple columns as a single column to generate a new dataset
 * thus the inner stage can fit on this dataset. This is design for unsupervised learning stage which may use both
 * source artifact and target artifact
 */
public class UnsupervisedStage extends Estimator {
    private static final long serialVersionUID = 7000401544310981006L;

    public UnsupervisedStage(PipelineStage innerStage) {

    }

    @Override
    public Model fit(Dataset dataset) {
        return null;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return null;
    }

    @Override
    public Estimator copy(ParamMap paramMap) {
        return null;
    }

    @Override
    public String uid() {
        return serialVersionUID;
    }
}
