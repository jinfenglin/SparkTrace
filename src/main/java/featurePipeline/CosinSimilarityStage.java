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
    private static final String OUTPUT_COLUMN_NAME = "cosin_similarity";

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        dataset.sqlContext().udf().register(COSIN_SIMILAIRY_UDF, (Vector v1, Vector v2) -> {
            SparseVector sv1 = (SparseVector) v1;
            SparseVector sv2 = (SparseVector) v2;
            int[] sv1Indices = sv1.indices();
            double[] sv1Value = sv1.values();
            int[] sv2Indices = sv2.indices();
            double[] sv2Value = sv2.values();

            int v1Length = sv1Indices.length;
            int v2Length = sv2Indices.length;

            double productScore = 0;
            double v1SquSum = 0;
            double v2SquSum = 0;
            for (int v1IndexCursor = 0, v2IndexCursor = 0; v1IndexCursor < v1Length || v2IndexCursor < v2Length; ) {

                while (sv1Indices[v1IndexCursor] < sv2Indices[v2IndexCursor] && v1IndexCursor < v1Length) {
                    v1IndexCursor += 1;
                    int curIndex = sv1Indices[v1IndexCursor];
                    v1SquSum += sv1Value[curIndex] * sv1Value[curIndex];
                }
                int v1TopIndex = sv1Indices[v1IndexCursor];
                int v2TopIndex = sv2Indices[v2IndexCursor];
                if (v1TopIndex == v2TopIndex) {
                    productScore += sv1Value[v1TopIndex] * sv2Value[v2TopIndex];
                    v1SquSum += sv1Value[v1TopIndex] * sv1Value[v1TopIndex];
                    v2SquSum += sv2Value[v2TopIndex] * sv2Value[v2TopIndex];
                }
                while (sv1Indices[v1IndexCursor] > sv2Indices[v2IndexCursor] & v2IndexCursor < v2Length) {
                    v2IndexCursor += 1;
                    int curIndex = sv1Indices[v2IndexCursor];
                    v2SquSum += sv2Value[curIndex] * sv2Value[curIndex];
                }
            }


            return productScore / (Math.sqrt(v1SquSum) * Math.sqrt(v2SquSum));
        }, DataTypes.DoubleType);
        Column col1 = dataset.col("vec1");
        Column col2 = dataset.col("vec2");
        Column outputCol = functions.callUDF(COSIN_SIMILAIRY_UDF, col1, col2);
        return dataset.withColumn(OUTPUT_COLUMN_NAME, outputCol);
    }

    @Override
    public StructType transformSchema(StructType structType) {
        structType.add("cosine_similarity", DataTypes.DoubleType, false);
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

}
