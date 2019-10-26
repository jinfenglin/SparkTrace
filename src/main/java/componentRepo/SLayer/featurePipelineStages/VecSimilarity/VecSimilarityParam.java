package componentRepo.SLayer.featurePipelineStages.VecSimilarity;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public interface VecSimilarityParam extends HasInputCols, HasOutputCol {
    static final String[] DEFAULT_INPUT_COLS = new String[]{"vector1", "vector2"};
    static final String DEFAULT_OUTPUT_COL = "cosin_similarity";

    default StringArrayParam initInputCols() {
        StringArrayParam inputCols = new StringArrayParam(this, "inputCols", "Input columns which contain sparse vectros ");
        setDefault(inputCols, DEFAULT_INPUT_COLS);
        return inputCols;
    }

    default Param<String> initOutputCol() {
        Param<String> outputCol = new Param<String>(this, "outputCol", "output column for cosin similarity of two vectors");
        setDefault(outputCol, DEFAULT_OUTPUT_COL);
        return outputCol;
    }

    @Override
    default Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    default void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {

    }

    @Override
    default String[] getInputCols() {
        return getOrDefault(inputCols());
    }

    @Override
    default void org$apache$spark$ml$param$shared$HasOutputCol$_setter_$outputCol_$eq(Param param) {

    }

    @Override
    default String getOutputCol() {
        return getOrDefault(outputCol());
    }


    default void setInputCols(String vec1, String vec2) {
        set(inputCols(), new String[]{vec1, vec2});
    }

    default void setOutputCol(String colName) {
        set(outputCol(), colName);
    }


    default StructType addSimilarityToSchema(StructType structType) {
        StructField similarityField = new StructField(getOutputCol(), DataTypes.DoubleType, false, null);
        return structType.add(similarityField);
    }
}
