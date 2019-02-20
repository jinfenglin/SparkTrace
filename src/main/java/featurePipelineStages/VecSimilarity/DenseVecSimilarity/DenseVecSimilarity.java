package featurePipelineStages.VecSimilarity.DenseVecSimilarity;

import featurePipelineStages.VecSimilarity.VecSimilarityTransformer;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;

/**
 *
 */
public class DenseVecSimilarity extends VecSimilarityTransformer {
    private static final long serialVersionUID = 6115441137337791235L;
    private static final String COSIN_SIMILAIRY_UDF = "cosin_similarity_UDF";
    Param<String> outputCol;
    StringArrayParam inputCols;

    public DenseVecSimilarity() {
        super();
        inputCols = initInputCols();
        outputCol = initOutputCol();
    }


    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        transformSchema(dataset.schema());
        dataset.sqlContext().udf().register(COSIN_SIMILAIRY_UDF, (DenseVector v1, DenseVector v2) -> {
            double productScore = 0;
            double v1SquSum = 0;
            double v2SquSum = 0;
            assert v1.size() == v2.size();
            for (int i = 0; i < v1.size(); i++) {
                double v1Num = v1.apply(i);
                double v2Num = v2.apply(i);
                v1SquSum += v1Num * v1Num;
                v2SquSum += v2Num * v2Num;
                productScore += v1Num * v2Num;
            }
            return productScore / (Math.sqrt(v1SquSum) * Math.sqrt(v2SquSum));
        }, DataTypes.DoubleType);
        return getSimilarityScore(dataset, COSIN_SIMILAIRY_UDF);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
