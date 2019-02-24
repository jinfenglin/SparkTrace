package featurePipelineStages.VecSimilarity.SparseVecSimilarity;

import featurePipelineStages.VecSimilarity.VecSimilarityParam;
import featurePipelineStages.VecSimilarity.VecSimilarityTransformer;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.linalg.SparseVector;
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

import java.util.logging.Logger;

import static org.apache.spark.sql.functions.callUDF;

public class SparseVecCosinSimilarityStage extends VecSimilarityTransformer {
    private static final long serialVersionUID = 5667889784880518528L;
    private static final String COSIN_SIMILAIRY_UDF = "cosin_similarity_UDF";


    public SparseVecCosinSimilarityStage() {
        super();
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
        Logger.getLogger(this.getClass().getName()).info(String.format("finished sparseVecsim with "));
        return getSimilarityScore(dataset, COSIN_SIMILAIRY_UDF);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
