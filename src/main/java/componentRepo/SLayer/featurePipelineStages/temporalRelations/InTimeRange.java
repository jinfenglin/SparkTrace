package componentRepo.SLayer.featurePipelineStages.temporalRelations;

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

import java.util.Date;

import static componentRepo.SLayer.featurePipelineStages.temporalRelations.TimeDiff.parseTimeStamp;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 *
 */
public class InTimeRange extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = 8199707556342047548L;
    private static final String IN_RANGE = "IN_RANGE";
    StringArrayParam inputCols;
    Param<String> outputCol;

    public InTimeRange() {
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "3 columns of datatime for comparision. including lowerBound, givenTime, upperBound");
    }

    public Param<String> initOutputCol() {
        return new Param<String>(this, "outputCol", "outputColumn if the columns the result of mining col1 with col2, the unit is days");
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        dataset.sqlContext().udf().register(IN_RANGE, (String lower, String givenTime, String upper) -> {
            Date lowerTime = parseTimeStamp(lower);
            Date time = parseTimeStamp(givenTime);
            Date upperTime = parseTimeStamp(upper);
            return time.after(lowerTime) && time.before(upperTime);
        }, DataTypes.BooleanType);
        String[] inputs = getInputCols();
        Column lower = col(inputs[0]);
        Column time = col(inputs[1]);
        Column upper = col(inputs[2]);
        return dataset.withColumn(getOutputCol(), callUDF(IN_RANGE, lower, time, upper));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField field = new StructField(getOutputCol(), DataTypes.BooleanType, false, null);
        return structType.add(field);
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
