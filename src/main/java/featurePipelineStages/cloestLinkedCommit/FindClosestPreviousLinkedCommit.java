package featurePipelineStages.cloestLinkedCommit;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.UnaryTransformer;
import org.apache.spark.ml.param.BooleanParam;
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
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Date;
import java.util.Map;

import static featurePipelineStages.temporalRelations.TimeDiff.parseTimeStamp;
import static org.apache.spark.sql.functions.callUDF;
import static utils.BroadcastUtils.collectAsBroadcastMap;

public class FindClosestPreviousLinkedCommit extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = 2282760203752748258L;
    private static final String CLC = "CLC";
    StringArrayParam inputCols;
    Param<String> outputCol;
    BooleanParam isPreviousClosest;

    public FindClosestPreviousLinkedCommit() {
        inputCols = initInputCols();
        outputCol = initOutputCol();
        isPreviousClosest = initPrevious();
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "columns for calculating the closest previous commit, including  commit_id, commit_data, linked_commits");
    }

    public Param<String> initOutputCol() {
        return new Param<>(this, "outputCol", "outputColumn if the columns are equal then return 1 else return 0");
    }

    public BooleanParam initPrevious() {
        return new BooleanParam(this, "isPreviousClosest", "true = find closest previous commit, false = closest subsequent commit");
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String[] inputs = getInputCols();
        String commitIdCol = inputs[0];
        String dateCol = inputs[1];
        String linkedCommitsCol = inputs[2];
        Broadcast<Map<String, String>> br = collectAsBroadcastMap(dataset, commitIdCol, dateCol);
        dataset.sqlContext().udf().register(CLC, (String commitId, String commitTime, Seq<String> linkedCommits) -> {
            long minTime = Long.MAX_VALUE;
            String clc = "";
            for (String lc : JavaConverters.asJavaCollectionConverter(linkedCommits).asJavaCollection()) {
                if (br.value().containsKey(lc)) {
                    String lcCommitTime = br.value().get(lc);
                    Date lcDate = parseTimeStamp(lcCommitTime);
                    Date commitDate = parseTimeStamp(commitTime);
                    boolean isPrevious = (boolean) getOrDefault(isPreviousClosest);
                    long timeInterval = lcDate.getTime() - commitDate.getTime();
                    if ((isPrevious && timeInterval < 0) || (!isPrevious && timeInterval > 0)) {
                        if (minTime > timeInterval) {
                            minTime = timeInterval;
                            clc = lc;
                        }
                    }
                }
            }

            return clc;
        }, DataTypes.StringType);
        Column c1 = dataset.col(commitIdCol);
        Column c2 = dataset.col(dateCol);
        Column c3 = dataset.col(linkedCommitsCol);
        return dataset.withColumn(getOutputCol(), callUDF(CLC, c1, c2, c3));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField cpc = new StructField(getOutputCol(), DataTypes.StringType, false, null);
        return structType.add(cpc);
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
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
}
