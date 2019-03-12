package featurePipelineStages.temporalRelations;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.*;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Date;

import static org.apache.spark.sql.functions.callUDF;

/**
 *
 */
public class CompareThreshold extends Transformer implements HasInputCol, HasOutputCol {
    private static final long serialVersionUID = -3530114640064444003L;
    private static final String COMP_THRE = "COMP_THRE";
    DoubleParam threshold;
    Param<String> inputCol, outputCol;


    public CompareThreshold() {
        threshold = initThreshold();
        inputCol = initInputCol();
        outputCol = initOutputCol();
    }

    public Param<String> initInputCol() {
        return new Param<String>(this, "inputCol", "column with double number");
    }

    public Param<String> initOutputCol() {
        return new Param<String>(this, "outputCol", "if number is above the threshold return 1, else return 0");
    }

    public DoubleParam initThreshold() {
        return new DoubleParam(this, "threshold", "threshold for comparasion");
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        dataset.sqlContext().udf().register(COMP_THRE, new UDF1<Double, Integer>() {
            @Override
            public Integer call(Double aDouble) throws Exception {
                if (aDouble > (double) getOrDefault(threshold)) {
                    return 1;
                }
                return 0;
            }
        }, DataTypes.IntegerType);
        return dataset.withColumn(getOutputCol(), callUDF(COMP_THRE, dataset.col(getInputCol())));
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField field = new StructField(getOutputCol(), DataTypes.IntegerType, false, null);
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
