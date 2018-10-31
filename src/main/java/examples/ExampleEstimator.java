package examples;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;

public class ExampleEstimator extends Estimator {
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
        return null;
    }
}
