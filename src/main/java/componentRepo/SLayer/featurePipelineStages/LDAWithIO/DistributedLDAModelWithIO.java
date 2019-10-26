package componentRepo.SLayer.featurePipelineStages.LDAWithIO;

import org.apache.spark.ml.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.sql.SparkSession;
import scala.Option;


/**
 *
 */
public class DistributedLDAModelWithIO extends DistributedLDAModel implements LDAIOParam {

    public DistributedLDAModelWithIO(String uid, int vocabSize, org.apache.spark.mllib.clustering.DistributedLDAModel oldDistributedModel, SparkSession sparkSession, Option<LocalLDAModel> oldLocalModelOption) {
        super(uid, vocabSize, oldDistributedModel, sparkSession, oldLocalModelOption);
    }
}
