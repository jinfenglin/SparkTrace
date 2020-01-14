package componentRepo.SLayer.featurePipelineStages.cleanStage;

import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.shared.HasInputCol;

import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.callUDF;

/**
 * Clean text by removing replacing non alphabetic characters with white space
 */
public class CleanStage extends Transformer implements HasInputCol, HasOutputCol {
    private static final long serialVersionUID = -7547398017958186201L;
    Param<String> inputCol, outputCol;
    private static final String CLEAN_STAGE = "CLEAN_STAGE";

    public Param<String> initInputCol() {
        return new Param<>(this, "inputCol", "text to be cleaned");
    }

    public Param<String> initOutputCol() {
        return new Param<>(this, "outputCol", "text in string after cleaning");
    }

    public CleanStage() {
        inputCol = initInputCol();
        outputCol = initOutputCol();
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String input = getInputCol();
        dataset.sqlContext().udf().register(CLEAN_STAGE, (String text) -> {
            text = text.replaceAll("[^A-Za-z]", " ");
            text = text.replaceAll("\\s\\w\\s", " ");
            text = text.replaceAll("\\s+", " ");
            return text;
        }, DataTypes.StringType);
        return dataset.withColumn(getOutputCol(), callUDF(CLEAN_STAGE, dataset.col(input)));
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
    public void org$apache$spark$ml$param$shared$HasInputCol$_setter_$inputCol_$eq(Param param) {

    }

    @Override
    public Param<String> inputCol() {
        return inputCol;
    }

    @Override
    public String getInputCol() {
        return getOrDefault(inputCol);
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
