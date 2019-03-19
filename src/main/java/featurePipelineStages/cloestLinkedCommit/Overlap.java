package featurePipelineStages.cloestLinkedCommit;

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
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static utils.BroadcastUtils.collectAsBroadcastMapWithSeqValue;

/**
 *
 */
public class Overlap extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = -6347415300401015723L;
    private static String OVERLAP = "OVERLAP";
    StringArrayParam inputCols;
    Param<String> outputCol;

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "columns for calculating the overlapping between current commit and closest previous/subsequent commit, including  commit_id, files, target_commit");
    }

    public Param<String> initOutputCol() {
        return new Param<>(this, "outputCol", "outputColumn if the columns are equal then return 1 else return 0");
    }

    public Overlap() {
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }


    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String[] inputs = getInputCols();
        String commidIdCol = inputs[0];
        String filesCol = inputs[1];
        String targetCommitCol = inputs[2];
        Broadcast<Map<String, Seq<String>>> br = collectAsBroadcastMapWithSeqValue(dataset, commidIdCol, filesCol);
        dataset.sqlContext().udf().register(OVERLAP, (String id1, String id2) -> {
            if (id2.length() > 0) {
                Seq<String> file1 = br.getValue().get(id1);
                Seq<String> file2 = br.getValue().get(id2);
                if (file1 == null || file2 == null) {
                    return 0.0;
                }
                Set<String> s1 = new HashSet<>(JavaConverters.asJavaCollectionConverter(file1).asJavaCollection());
                Set<String> s2 = new HashSet<>(JavaConverters.asJavaCollectionConverter(file2).asJavaCollection());
                long total = s1.size() + s2.size();
                s1.retainAll(s2);
                long shared = s1.size();
                return shared / Double.valueOf(total);
            } else {
                return 0.0;
            }
        }, DoubleType);
        Column c1 = dataset.col(commidIdCol);
        Column c2 = dataset.col(targetCommitCol);
        return dataset.withColumn(getOutputCol(), callUDF(OVERLAP, c1, c2));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField field = new StructField(getOutputCol(), DoubleType, false, null);
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
