package featurePipelineStages.sameColumnStage;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.callUDF;

/**
 * Given two column
 */
public class SameAuthorStage extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = -583773311796744634L;
    private static final String IS_EQUAL = "IS_EQUAL";
    StringArrayParam inputCols;
    Param<String> outputCol;


    public SameAuthorStage() {
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "two columns for compare");
    }

    public Param<String> initOutputCol() {
        return new Param<String>(this, "outputCol", "outputColumn if the columns are equal then return 1 else return 0");
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        dataset.sqlContext().udf().register(IS_EQUAL, (String s1, String s2) -> {
            if (s1.equals(s2)) {
                return 1;
            } else {
                return 0;
            }
        }, DataTypes.IntegerType);
        String[] inputColumns = getInputCols();
        Column c1 = dataset.col(inputColumns[0]);
        Column c2 = dataset.col(inputColumns[1]);
        return dataset.withColumn(getOutputCol(), callUDF(IS_EQUAL, c1, c2));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField isEqual = new StructField(getOutputCol(), DataTypes.IntegerType, false, null);
        return structType.add(isEqual);
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {

    }

    @Override
    public StringArrayParam inputCols() {
        return inputCols;
    }

    @Override
    public String[] getInputCols() {
        return getOrDefault(inputCols);
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
        return getOrDefault(outputCol);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
