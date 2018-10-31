package featurePipeline;


import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class SDFStage extends Transformer {
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return null;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return null;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return null;
    }
}
