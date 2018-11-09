package featurePipeline;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class CosinSimilarityStage extends Transformer {
    private static final long serialVersionUID = 5667889784880518528L;
    private static final String COSIN_SIMILAIRY_UDF = "cosin_similarity";
    private String outputCol;
    private String inputCol1, inputCol2;

    public CosinSimilarityStage() {
        super();
        setInputCol1("vector1");
        setInputCol2("vector2");
        setOutputCol("cosin_similarity");

    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
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
        Column col1 = dataset.col(this.inputCol1);
        Column col2 = dataset.col(this.inputCol2);
        Column outputColumn = functions.callUDF(COSIN_SIMILAIRY_UDF, col1, col2);
        return dataset.withColumn(getOutputCol(), outputColumn);
    }

    @Override
    public StructType transformSchema(StructType structType) {
        structType.add(outputCol, DataTypes.DoubleType, false);
        return null;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return this.getClass().getName() + serialVersionUID;
    }

    public String getOutputCol() {
        return outputCol;
    }

    public CosinSimilarityStage setOutputCol(String outputCol) {
        this.outputCol = outputCol;
        return this;
    }

    public String getInputCol1() {
        return inputCol1;
    }

    public CosinSimilarityStage setInputCol1(String inputCol1) {
        this.inputCol1 = inputCol1;
        return this;
    }

    public String getInputCol2() {
        return inputCol2;
    }

    public CosinSimilarityStage setInputCol2(String inputCol2) {
        this.inputCol2 = inputCol2;
        return this;
    }
}
