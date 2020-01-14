package componentRepo.SLayer.featurePipelineStages.VecSimilarity.SparseVecSimilarity;

import componentRepo.SLayer.featurePipelineStages.VecSimilarity.VecSimilarityTransformer;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.util.logging.Logger;

public class SparseVecCosinSimilarityStage extends VecSimilarityTransformer {
    private static final long serialVersionUID = 5667889784880518528L;
    private static final String COSIN_SIMILARITY_UDF = "cosin_similarity_UDF";

    public SparseVecCosinSimilarityStage() {
        super();
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        transformSchema(dataset.schema());
        dataset.sqlContext().udf().register(COSIN_SIMILARITY_UDF, (SparseVector v1, SparseVector v2) -> {
            int[] sv1Indices = v1.indices();
            double[] sv1Value = v1.values();
            int[] sv2Indices = v2.indices();
            double[] sv2Value = v2.values();

            int v1Length = sv1Indices.length;
            int v2Length = sv2Indices.length;

            double productScore = 0;
            double v1SquSum = 0;
            double v2SquSum = 0;
            for (int i1 = 0, i2 = 0; i1 < v1Length || i2 < v2Length; ) {
                while (i1 < v1Length && (i2 >= v2Length || sv1Indices[i1] < sv2Indices[i2])) {
                    v1SquSum += sv1Value[i1] * sv1Value[i1];
                    i1 += 1;
                }
                while (i2 < v2Length && (i1 >= v1Length || sv1Indices[i1] > sv2Indices[i2])) {
                    v2SquSum += sv2Value[i2] * sv2Value[i2];
                    i2 += 1;
                }
                if (i1 < v1Length && i2 < v2Length && sv1Indices[i1] == sv2Indices[i2]) {
                    productScore += sv1Value[i1] * sv2Value[i2];
                    v1SquSum += sv1Value[i1] * sv1Value[i1];
                    v2SquSum += sv2Value[i2] * sv2Value[i2];
                    i1 += 1;
                    i2 += 1;
                }
            }
            double deno = Math.sqrt(v1SquSum) * Math.sqrt(v2SquSum);
            double sim = 0;
            if (deno > 0) {
                sim = productScore / deno;
            }
            return sim;
        }, DataTypes.DoubleType);
        return getSimilarityScore(dataset, COSIN_SIMILARITY_UDF);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
