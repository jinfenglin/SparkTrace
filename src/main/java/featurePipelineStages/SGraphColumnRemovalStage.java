package featurePipelineStages;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.logging.Logger;


/**
 * A pipeline stage that remove columns from the dataset
 */
public class SGraphColumnRemovalStage extends Transformer implements HasInputCols {
    private static final long serialVersionUID = -2423089917485389460L;
    StringArrayParam inputCols;

    public SGraphColumnRemovalStage() {
        inputCols = initInputCols();
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String[] cols = getInputCols();
        for (String colName : cols) {
            dataset = dataset.drop(colName);
        }
        return (Dataset<Row>) dataset;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        String[] cols = getInputCols();
        for (String colName : cols) {
            //assert structType.contains(colName);
            Logger.getLogger("column remover").info("removing column " + colName);
            structType.drop(structType.fieldIndex(colName));
        }
        return structType;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {

    }


    @Override
    public String uid() {
        return this.getClass().getName() + serialVersionUID;
    }

    @Override
    public StringArrayParam inputCols() {
        return inputCols;
    }

    private StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "input columns for a vertex in SGraph");
    }

    @Override
    public String[] getInputCols() {
        return getOrDefault(inputCols);
    }

    public SGraphColumnRemovalStage setInputCols(String[] inputColArray) {
        set(inputCols, inputColArray);
        return this;
    }

    @Override
    public String toString() {
        return String.join(",", Arrays.asList(getInputCols()));
    }
}
