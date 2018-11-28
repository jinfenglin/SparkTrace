package featurePipeline;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.types.StructType;

public class TraceSparkStage extends PipelineStage {
    @Override
    public StructType transformSchema(StructType structType) {
        structType.fieldNames();

        return null;
    }

    @Override
    public PipelineStage copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return null;
    }
}
