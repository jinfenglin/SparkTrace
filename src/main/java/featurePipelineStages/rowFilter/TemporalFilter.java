package featurePipelineStages.rowFilter;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.ml.param.shared.HasOutputCols;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Calendar;
import java.util.Date;

import static featurePipelineStages.temporalRelations.TimeDiff.parseTimeStamp;
import static org.apache.spark.sql.functions.callUDF;

/**
 * FIXME
 * this is a work around for current status of implementation. Row level operation should be specified at the top level graph not in tbe second level.
 * However, we don't have the top level implementation yet. As a result, we hard code the columns that must be contained as source_time, target_start_time and target_end_time.
 * Rows where target_start_time< source_time< target_end_time will be keep, the rest will be abandon. To reserve the generality of the framework, we specify the first 3 input columns
 * as our time data.
 * 0: source_time
 * 1: target_start_time
 * 2:target_end_time
 */
public class TemporalFilter extends Transformer implements HasInputCols, HasOutputCols {
    private static final long serialVersionUID = 1603357492539477276L;
    private static final String TIME_CONDITION = "TIME_CONDITION";
    private static final String IS_INSTANCE = "IS_INSTANCE";
    StringArrayParam inputCols, outputCols;
    public TemporalFilter() {
        inputCols = initInputCols();
        outputCols = initOutputCols();
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "input cols ");
    }

    public StringArrayParam initOutputCols() {
        return new StringArrayParam(this, "outputCols", "outputCols");
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String[] inputColumns = getInputCols();
        Column c1 = dataset.col(inputColumns[0]);
        Column c2 = dataset.col(inputColumns[1]);
        Column c3 = dataset.col(inputColumns[2]);
        dataset.sqlContext().udf().register(TIME_CONDITION, (String sTime, String tStart, String tEnd) -> {
            Date sTimeDate = parseTimeStamp(sTime);
            Date tStartTime = parseTimeStamp(tStart);
            Date tEndTime = parseTimeStamp(tEnd) ;

            return sTimeDate.getTime() <= tEndTime.getTime() && sTimeDate.getTime()> tStartTime.getTime();
        }, DataTypes.BooleanType);
        //dataset = dataset.withColumn(IS_INSTANCE,callUDF(TIME_CONDITION, c1, c2, c3));
        dataset = dataset.filter(c1.between(c2,c3));
        //dataset = dataset.filter(dataset.col(IS_INSTANCE).equalTo(true));
        return (Dataset<Row>) dataset;
    }

    @Override
    public StructType transformSchema(StructType structType) {
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
    public StringArrayParam inputCols() {
        return this.inputCols;
    }

    @Override
    public String[] getInputCols() {
        return getOrDefault(inputCols);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasOutputCols$_setter_$outputCols_$eq(StringArrayParam stringArrayParam) {

    }

    @Override
    public StringArrayParam outputCols() {
        return this.outputCols;
    }

    @Override
    public String[] getOutputCols() {
        return getOrDefault(outputCols);
    }
}
