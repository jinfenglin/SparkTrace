package featurePipeline.NullRemoveWrapper;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.NoSuchElementException;
import java.util.logging.Logger;

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
        inputCol = initInputCol();
        outputCol = initOutputCol();
        setInnerEstimator(innerEstimator);
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

    private Estimator getInnerEstimator() {
        return innerEstimator.copy(org$apache$spark$ml$param$Params$$paramMap());
    }

    private void setInnerEstimator(Estimator estimator) {
        setInnerStage(estimator);
        try {
            setInputCol(getInnerInputCol());
            setOutputCol(getInnerOutputCol());
        } catch (NoSuchElementException e) {
            setInputCol(null);
            setOutputCol(null);
            Logger.getLogger(this.getClass().getName()).warning(String.format("No default value for stage %s, its NullRemover's IO params are set to null", getInnerStage().uid()));
        }
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

    @Override
    public Param<String> inputCol() {
        return inputCol;
    }

    @Override
    public Param<String> outputCol() {
        return outputCol;
    }

    @Override
    public PipelineStage getInnerStage() {
        return getInnerEstimator();
    }

    @Override
    public void setInnerStage(PipelineStage stage) {
        this.innerEstimator = (Estimator) stage;
    }
}
