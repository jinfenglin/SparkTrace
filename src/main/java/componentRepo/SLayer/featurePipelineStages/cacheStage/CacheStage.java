package componentRepo.SLayer.featurePipelineStages.cacheStage;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 *
 */
public class CacheStage extends Transformer {
    private static final long serialVersionUID = 7009635289538569421L;

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return (Dataset<Row>) dataset.cache();
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return structType;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
