package componentRepo.SLayer.featurePipelineStages.cloestLinkedCommit;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.BroadcastUtils;

import java.util.*;

import static componentRepo.SLayer.featurePipelineStages.temporalRelations.TimeDiff.parseTimeStamp;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 *
 */
public class CLTimeDiff extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = 6236895304288111051L;
    private static final String CLTimeDiff = "CLTimeDiff";
    StringArrayParam inputCols;
    Param<String> outputCol;

    public CLTimeDiff() {
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "columns for calculating the closest commit time between current commit and previous commit, including  commit_id, commit_date, closestPreviousCommit");
    }

    public Param<String> initOutputCol() {
        return new Param<>(this, "outputCol", "outputColumn: the time differ between two commit");
    }


    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String[] inputs = getInputCols();
        String commitId = inputs[0];
        String commitDate = inputs[1];
        String previousCommitId = inputs[2];
        Broadcast<Map<String, String>> br = BroadcastUtils.collectAsBroadcastMap(dataset, commitId, commitDate);
        dataset.sqlContext().udf().register(CLTimeDiff, (String id1, String id2, String curTime) -> {
            if (id2.length() > 0) {
                Date time2 = parseTimeStamp(br.getValue().get(id2));
                Date time1 = parseTimeStamp(curTime);
                return time1.getTime() - time2.getTime();
            } else {
                return Long.valueOf(0);
            }
        }, LongType);

        Column c1 = dataset.col(commitId);
        Column c2 = dataset.col(previousCommitId);
        Column c3 = dataset.col(commitDate);
        return dataset.withColumn(getOutputCol(), callUDF(CLTimeDiff, c1, c2, c3));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField field = new StructField(getOutputCol(), LongType, false, null);
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
