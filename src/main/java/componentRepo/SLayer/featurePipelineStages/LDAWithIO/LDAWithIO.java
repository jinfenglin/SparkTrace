package componentRepo.SLayer.featurePipelineStages.LDAWithIO;

import org.apache.spark.ml.clustering.DistributedLDAModel;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.clustering.LocalLDAModel;
import org.apache.spark.sql.Dataset;
import scala.Option;

public class LDAWithIO extends LDA implements LDAIOParam {
    public LDAModel fit(Dataset<?> dataset) {
        LDAModel originModel = super.fit(dataset);
        LDAIOParam modelWIthIO;
        if (originModel instanceof LocalLDAModel) {
            modelWIthIO = new LocalLDAModelWithIO(uid(), originModel.vocabSize(), originModel.oldLocalModel(), dataset.sparkSession());
        } else {
            org.apache.spark.mllib.clustering.DistributedLDAModel distributedLDAModel = ((DistributedLDAModel) originModel).org$apache$spark$ml$clustering$DistributedLDAModel$$oldDistributedModel();
            modelWIthIO = new DistributedLDAModelWithIO(uid(), originModel.vocabSize(), distributedLDAModel, dataset.sparkSession(), Option.empty());
        }
        return (LDAModel) this.copyValues(modelWIthIO, this.extractParamMap());
    }

    public LDAWithIO() {
        super();
    }

}
