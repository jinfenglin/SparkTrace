package componentRepo.SLayer.featurePipelineStages.VecSimilarity.SparseVecSimilarity;

import componentRepo.SLayer.featurePipelineStages.VecSimilarity.VecSimilarityTransformer;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.util.logging.Logger;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class SparseVecCosinSimilarityStage extends VecSimilarityTransformer {
    private static final long serialVersionUID = 5667889784880518528L;
    private static final String COSIN_SIMILAIRY_UDF = "cosin_similarity_UDF";


    public SparseVecCosinSimilarityStage() {
        super();
    }

    public double cosine(SparseVector v1, SparseVector v2) {
        DenseVector dv1 = v1.toDense();
        DenseVector dv2 = v2.toDense();
        double sqt1 = 0, sqt2 = 0;
        double sum = 0;
        for (int i = 0; i < dv1.size(); i++) {
            double s1 = dv1.apply(i);
            double s2 = dv2.apply(i);
            sum += s1 * s2;
            sqt1 += s1 * s1;
            sqt2 += s2 * s2;
        }
        return sum / Math.sqrt(sqt1 * sqt2);
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        transformSchema(dataset.schema());
        dataset.sqlContext().udf().register(COSIN_SIMILAIRY_UDF, (String code_id, String req_id, SparseVector v1, SparseVector v2) -> {
            if (code_id.equals("OOPSGUIT.m") && req_id.equals("WV-HPS-VIM-012")) {
                System.out.println("were are here");
            }

            int[] sv1Indices = v1.indices();
            double[] sv1Value = v1.values();
            int[] sv2Indices = v2.indices();
            double[] sv2Value = v2.values();

            int v1Length = sv1Indices.length;
            int v2Length = sv2Indices.length;

            double productScore = 0;
            double v1SquSum = 0;
            double v2SquSum = 0;
            int cnt = 0;
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
                    cnt += 1;
                }
            }
            if (code_id.equals("OOPSGUIT.m") && req_id.equals("WV-HPS-VIM-012")) {
                System.out.println("were are here");
                assert cosine(v1, v2) == productScore / (Math.sqrt(v1SquSum) * Math.sqrt(v2SquSum));
            }
            return productScore / (Math.sqrt(v1SquSum) * Math.sqrt(v2SquSum));
        }, DataTypes.DoubleType);
        Logger.getLogger(this.getClass().getName()).info(String.format("finished sparseVecsim"));

        String[] inputColumns = getInputCols();
        Dataset<Row> tmp = dataset.select("*"); //Create a copy, unknown error happen when the operation is applied on origin dataset
        Column col1 = dataset.col(inputColumns[0]);
        Column col2 = dataset.col(inputColumns[1]);
        Column code_id_col = tmp.col("code_id");
        Column req_id_col = tmp.col("req_id");
        Column outputColumn = callUDF(COSIN_SIMILAIRY_UDF, code_id_col, req_id_col, col1, col2);
        return tmp.withColumn(getOutputCol(), outputColumn);
        // return getSimilarityScore(dataset, COSIN_SIMILAIRY_UDF);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
