package featurePipeline;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.ml.util.SchemaUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

import static org.apache.spark.sql.functions.callUDF;

public class CosinSimilarityStage extends Transformer implements HasInputCols, HasOutputCol {
    private static final long serialVersionUID = 5667889784880518528L;
    private static final String COSIN_SIMILAIRY_UDF = "cosin_similarity_UDF";
    private static final String[] DEFAULT_INPUT_COLS = new String[]{"vector1", "vector2"};
    private static final String DEFAULT_OUTPUT_COL = "cosin_similarity";
    Param<String> outputCol;
    StringArrayParam inputCols;

    public CosinSimilarityStage() {
        super();
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }

    private StringArrayParam initInputCols() {
        StringArrayParam inputCols = new StringArrayParam(this, "inputCols", "Input columns which contain sparse vectros ");
        setDefault(inputCols, DEFAULT_INPUT_COLS);
        return inputCols;
    }

    private Param<String> initOutputCol() {
        Param<String> outputCol = new Param<String>(this, "outputCol", "output column for cosin similarity of two vectors");
        setDefault(outputCol, DEFAULT_OUTPUT_COL);
        return outputCol;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        transformSchema(dataset.schema());
        dataset.sqlContext().udf().register(COSIN_SIMILAIRY_UDF, (SparseVector v1, SparseVector v2) -> {
            int[] sv1Indices = v1.indices();
            double[] sv1Value = v1.values();
            int[] sv2Indices = v2.indices();
            double[] sv2Value = v2.values();

            int v1Length = sv1Indices.length;
            int v2Length = sv2Indices.length;

            double productScore = 0;
            double v1SquSum = 0;
            double v2SquSum = 0;
            for (int v1IndexCursor = 0, v2IndexCursor = 0; v1IndexCursor < v1Length || v2IndexCursor < v2Length; ) {
                while (v1IndexCursor < v1Length && (v2IndexCursor >= v2Length || sv1Indices[v1IndexCursor] < sv2Indices[v2IndexCursor])) {
                    v1SquSum += sv1Value[v1IndexCursor] * sv1Value[v1IndexCursor];
                    v1IndexCursor += 1;
                }

                while (v2IndexCursor < v2Length && (v1IndexCursor >= v1Length || sv1Indices[v1IndexCursor] > sv2Indices[v2IndexCursor])) {
                    v2SquSum += sv2Value[v2IndexCursor] * sv2Value[v2IndexCursor];
                    v2IndexCursor += 1;
                }

                if (v1IndexCursor < v1Length && v2IndexCursor < v2Length && sv1Indices[v1IndexCursor] == sv2Indices[v2IndexCursor]) {
                    productScore += sv1Value[v1IndexCursor] * sv2Value[v2IndexCursor];
                    v1SquSum += sv1Value[v1IndexCursor] * sv1Value[v1IndexCursor];
                    v2SquSum += sv2Value[v2IndexCursor] * sv2Value[v2IndexCursor];
                    v1IndexCursor += 1;
                    v2IndexCursor += 1;
                }
            }
            return productScore / (Math.sqrt(v1SquSum) * Math.sqrt(v2SquSum));
        }, DataTypes.DoubleType);
        String[] inputColumns = getInputCols();

        Dataset<Row> tmp = dataset.select("*"); //Create a copy, unknown error happen when the operation is applied on origin dataset
        Column col1 = tmp.col(inputColumns[0]);
        Column col2 = tmp.col(inputColumns[1]);
        Column outputColumn = callUDF(COSIN_SIMILAIRY_UDF, col1, col2);
        return tmp.withColumn(getOutputCol(), outputColumn);
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructField similarityField = new StructField(getOutputCol(), DataTypes.DoubleType, false, null);
        return structType.add(similarityField);
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
        return getOrDefault(inputCols());
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
        return getOrDefault(outputCol());
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }

    public void setInputCols(String vec1, String vec2) {
        set(inputCols(), new String[]{vec1, vec2});
    }

    public void setOutputCol(String colName) {
        set(outputCol(), colName);
    }
}
