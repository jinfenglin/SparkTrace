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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.BroadcastUtils;

import java.util.Map;

import static org.apache.spark.sql.functions.callUDF;

/**
 *
 */
public class CLUser extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = 2366684289204297423L;
    private static final String CLUSER = "CLUSER";
    StringArrayParam inputCols;
    Param<String> outputCol;

    public CLUser() {
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "commit_id, commit_author, closestPreviousCommit");
    }

    public Param<String> initOutputCol() {
        return new Param<>(this, "outputCol", "the cloest previous commit author");
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String[] inputs = getInputCols();
        String commitId = inputs[0];
        String commitAuthor = inputs[1];
        String closestPreviousCommit = inputs[2];
        Broadcast<Map<String, String>> br = BroadcastUtils.collectAsBroadcastMap(dataset, commitId, commitAuthor);
        dataset.sqlContext().udf().register(CLUSER, (String pCommitId) -> {
            String author = br.getValue().get(pCommitId);
            return author;
        }, DataTypes.StringType);
        Column c1 = dataset.col(closestPreviousCommit);
        return  dataset.withColumn(getOutputCol(), callUDF(CLUSER, c1));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField field = new StructField(getOutputCol(), DataTypes.StringType, false, null);
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
