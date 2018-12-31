package featurePipeline.NullRemoveWrapper;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

/**
 * A wrapper class for any Estimator. This class will remove null value from the input dataset on given columns,
 * thus the inner estimator will expose to a dataset with no NULL value in its input columns. After
 */
public class NullRemoverEstimatorSingleIO extends Estimator<NullRemoverModelSingleIO> implements InnerStageImplementHasInputCol, InnerStageImplementHasOutputCol {
    private static final long serialVersionUID = -605176909840583442L;
    private Estimator innerEstimator;
    private Param<String> inputCol, outputCol;

    public NullRemoverEstimatorSingleIO(Estimator innerEstimator) {
        this.innerEstimator = innerEstimator;
        inputCol = initInputCol();
        outputCol = initOutputCol();
    }

    @Override
    public NullRemoverModelSingleIO fit(Dataset<?> dataset) {
        Dataset<Row> inputDataset = dataset.select(getInputCol());
        Dataset<Row> inputNoNull = inputDataset.where(col(getInputCol()).isNotNull());
        Transformer innerTransformer = innerEstimator.fit(inputNoNull);
        NullRemoverModelSingleIO model = new NullRemoverModelSingleIO(innerTransformer);
        model.setInputCol(getInputCol());
        model.setOutputCol(getOutputCol());
        return model;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return innerEstimator.transformSchema(structType);
    }

    @Override
    public Estimator<NullRemoverModelSingleIO> copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }

    public Estimator getInnerEstimator() {
        return innerEstimator;
    }

    public void setInnerEstimator(Estimator innerEstimator) {
        this.innerEstimator = innerEstimator;
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasInputCol$_setter_$inputCol_$eq(Param param) {
    }

    @Override
    public Param<String> inputCol() {
        return inputCol;
    }

    @Override
    public String getInputCol() {
        return getOrDefault(inputCol());
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasOutputCol$_setter_$outputCol_$eq(Param param) {

    }

    @Override
    public Param<String> outputCol() {
        return outputCol;
    }

    @Override
    public String getOutputCol() {
        return getOrDefault(outputCol());
    }

    @Override
    public NullRemoverEstimatorSingleIO setInputCol(String colName) {
        set(inputCol(), colName);
        return this;
    }

    @Override
    public NullRemoverEstimatorSingleIO setOutputCol(String colName) {
        set(outputCol(), colName);
        return this;
    }
}
