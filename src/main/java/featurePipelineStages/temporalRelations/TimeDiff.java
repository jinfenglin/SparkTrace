package featurePipelineStages.temporalRelations;

import javafx.scene.input.DataFormat;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.apache.spark.sql.functions.callUDF;

/**
 *
 */
public class TimeDiff extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = -110407388135240734L;
    private static final String TIME_DIFF = "TIME_DIFF";
    StringArrayParam inputCols;
    Param<String> outputCol;

    public TimeDiff() {
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "two columns of datatime for compare");
    }

    public Param<String> initOutputCol() {
        return new Param<String>(this, "outputCol", "outputColumn if the columns the result of mining col1 with col2, the unit is days");
    }

    private Date parseTimeStamp(String timeStamp) throws ParseException {
        SimpleDateFormat timeStampFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
        if (timeStamp.contains("EDT")) {
            TimeZone edtTime = TimeZone.getTimeZone("GMT-04:00");
            timeStampFormat.setTimeZone(edtTime);
        } else if (timeStamp.contains("EST")) {
            TimeZone estTime = TimeZone.getTimeZone("EST");
            timeStampFormat.setTimeZone(estTime);
        }
        Date timeStampDate = timeStampFormat.parse(timeStamp);
        return timeStampDate;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        double unit = 3600000 * 24;
        dataset.sqlContext().udf().register(TIME_DIFF, (String col1, String col2) -> {
            Date t1 = parseTimeStamp(col1);
            Date t2 = parseTimeStamp(col2);
            return (double) ((t1.getTime() - t2.getTime()) / unit);
        }, DataTypes.DoubleType);
        String[] inputColumns = getInputCols();
        return dataset.withColumn(getOutputCol(), callUDF(TIME_DIFF, dataset.col(inputColumns[0]), dataset.col(inputColumns[1])));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField timeDiffField = new StructField(getOutputCol(), DataTypes.DoubleType, false, null);
        return structType.add(timeDiffField);
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
