package featurePipeline.NullRemoveWrapper;

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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 *
 */
public class NullRemoverModelSingleIO extends Model<NullRemoverModelSingleIO> implements InnerStageImplementHasInputCol, InnerStageImplementHasOutputCol {
    private static final long serialVersionUID = -68163027255309458L;
    private Param<String> inputCol, outputCol;
    private Transformer innerTransformer;

    public NullRemoverModelSingleIO(Transformer innerTransformer) {
        this.innerTransformer = innerTransformer;
        inputCol = initInputCol();
        outputCol = initOutputCol();
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> inputDataset = dataset.toDF();
        Dataset<Row> inputNoNull = inputDataset.where(col(getInputCol()).isNotNull());
        Dataset<Row> inputHasNull = inputDataset.where(col(getInputCol()).isNull());
        inputHasNull = inputHasNull.withColumn(getOutputCol(), lit(null));
        Dataset<Row> result = innerTransformer.transform(inputNoNull);
        result = result.union(inputHasNull);
        result.show();
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
        return new NullRemoverModelSingleIO(getInnerTransformer()).setParent(parent());
    }


    public Transformer getInnerTransformer() {
        return innerTransformer;
    }

    public void setInnerTransformer(Transformer innerTransformer) {
        this.innerTransformer = innerTransformer;
    }

    @Override
    public NullRemoverModelSingleIO setInputCol(String colName) {
        set(inputCol(), colName);
        setInnerInputCol(colName);
        return this;
    }

    private void setInnerInputCol(String colName) {
        HasInputCol hasInputCol = (HasInputCol) getInnerTransformer();
        hasInputCol.set(hasInputCol.inputCol(), colName);
    }

    @Override
    public PipelineStage setOutputCol(String colName) {
        set(outputCol(), colName);
        setInnerOutputCol(colName);
        return this;
    }

    private void setInnerOutputCol(String colName) {
        HasOutputCol hasOutputCol = (HasOutputCol) getInnerTransformer();
        hasOutputCol.set(hasOutputCol.outputCol(), colName);
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasInputCol$_setter_$inputCol_$eq(Param param) {

    }

    @Override
    public Param<String> inputCol() {
        return this.inputCol;
    }

    @Override
    public String getInputCol() {
        return getOrDefault(inputCol());
    }

    private String getInnerInputCol() {
        HasInputCol hasInputCol = (HasInputCol) getInnerTransformer();
        return hasInputCol.getInputCol();
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasOutputCol$_setter_$outputCol_$eq(Param param) {

    }

    @Override
    public Param<String> outputCol() {
        return this.outputCol;
    }

    private String getInnerOutputCol() {
        HasOutputCol hasOutputCol = (HasOutputCol) getInnerTransformer();
        return hasOutputCol.getOutputCol();
    }

    @Override
    public String getOutputCol() {
        return getOrDefault(outputCol());
    }
}
