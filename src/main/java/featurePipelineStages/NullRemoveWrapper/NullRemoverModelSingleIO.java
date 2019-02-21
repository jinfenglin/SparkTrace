package featurePipelineStages.NullRemoveWrapper;

import org.apache.spark.ml.Model;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * A wrapper for normal estimators which will filter the null value from the input for inner stages.
 * Modification on outside class's IO params will be reflected into the inner stage.
 * If user wanna change other param of the inner stage should use the setInnerStage() method which will reset every
 * param including the IO params.
 */
public class NullRemoverModelSingleIO extends Model<NullRemoverModelSingleIO> implements InnerStageImplementHasInputCol, InnerStageImplementHasOutputCol {
    private static final long serialVersionUID = -68163027255309458L;
    private Param<String> inputCol, outputCol;
    private Transformer innerTransformer;

    public NullRemoverModelSingleIO(Transformer innerTransformer) {
        inputCol = initInputCol();
        outputCol = initOutputCol();
        assert innerTransformer instanceof HasInputCol;
        assert innerTransformer instanceof HasOutputCol;
        setInnerTransformer(innerTransformer);
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Logger.getLogger(this.getClass().getName()).info(String.format("running nullremover with inner stage %s", innerTransformer.getClass().getSimpleName()));
        Dataset<Row> inputDataset = dataset.toDF();
        Dataset<Row> inputNoNull = inputDataset.where(col(getInputCol()).isNotNull());
        Dataset<Row> inputHasNull = inputDataset.where(col(getInputCol()).isNull());
        inputHasNull = inputHasNull.withColumn(getOutputCol(), lit(null));
        Dataset<Row> result = innerTransformer.transform(inputNoNull);
        result = result.union(inputHasNull);
        return result;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return innerTransformer.transformSchema(structType);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }

    @Override
    public NullRemoverModelSingleIO copy(ParamMap paramMap) {
        return new NullRemoverModelSingleIO(getInnerTransformerCopy()).setParent(parent());
    }

    /**
     * Return a copy of inner transformer, modifications will not be reflected in the real one.
     *
     * @return
     */
    private Transformer getInnerTransformer() {
        return innerTransformer;
    }

    private Transformer getInnerTransformerCopy() {
        return innerTransformer.copy(org$apache$spark$ml$param$Params$$paramMap());
    }

    /**
     * Set a innerTransformer for this wrapper. The wrapper's inputCol and outputCol will be updated as well;
     *
     * @param innerTransformer
     */
    public void setInnerTransformer(Transformer innerTransformer) {
        setInnerStage(innerTransformer);
        syncInputCol();
        syncOutputCol();
    }

    @Override
    public Param<String> inputCol() {
        return this.inputCol;
    }

    @Override
    public Param<String> outputCol() {
        return this.outputCol;
    }

    @Override
    public PipelineStage getInnerStage() {
        return getInnerTransformer();
    }

    @Override
    public void setInnerStage(PipelineStage stage) {
        this.innerTransformer = (Transformer) stage;
    }
}
