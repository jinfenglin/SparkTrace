package componentRepo.SLayer.featurePipelineStages.VecSimilarity;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.callUDF;

/**
 *
 */
public abstract class VecSimilarityTransformer extends Transformer implements VecSimilarityParam {
    Param<String> outputCol;
    StringArrayParam inputCols;

    public VecSimilarityTransformer() {
        super();
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }

    protected Dataset<Row> getSimilarityScore(Dataset<?> dataset, String similarityUDFName) {
        String[] inputColumns = getInputCols();
        Dataset<Row> tmp = dataset.select("*"); //Create a copy, unknown error happen when the operation is applied on origin dataset
        Column col1 = tmp.col(inputColumns[0]);
        Column col2 = tmp.col(inputColumns[1]);
        Column outputColumn = callUDF(similarityUDFName, col1, col2);
        return tmp.withColumn(getOutputCol(), outputColumn);
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return addSimilarityToSchema(structType);
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public StringArrayParam inputCols() {
        return this.inputCols;
    }

    @Override
    public Param<String> outputCol() {
        return this.outputCol;
    }
}
