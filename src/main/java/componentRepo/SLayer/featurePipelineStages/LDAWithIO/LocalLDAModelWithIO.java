package componentRepo.SLayer.featurePipelineStages.LDAWithIO;

import org.apache.spark.ml.clustering.LocalLDAModel;
import org.apache.spark.sql.SparkSession;

/**
 *
 */
public class LocalLDAModelWithIO extends LocalLDAModel implements LDAIOParam {
    public LocalLDAModelWithIO(String uid, int vocabSize, org.apache.spark.mllib.clustering.LocalLDAModel oldLocalModel, SparkSession sparkSession) {
        super(uid, vocabSize, oldLocalModel, sparkSession);
    }
}
