package componentRepo.SLayer.featurePipelineStages;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

public class SkippableTransformer extends Transformer {
    private static final long serialVersionUID = 8448315237093573155L;
    private boolean skipStage;
    protected Transformer transformer;

    public SkippableTransformer(Transformer transformer) {
        this.transformer = transformer;
        skipStage = false;
    }

    @Override
    public Dataset<Row> transform(Dataset dataset) {
        if (!getSkipStage()) {
            return transformer.transform(dataset);
        } else {
            Logger.getLogger(this.getClass().getName()).info("Skipped Stage " + uid());
            return dataset;
        }
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructType transSchema = transformer.transformSchema(structType);
        Set<String> transFieldNames = new HashSet<>(Arrays.asList(transSchema.fieldNames()));
        Set<String> originFieldNames = new HashSet<>(Arrays.asList(structType.fieldNames()));
        if (transFieldNames.equals(originFieldNames)) {
            //if transformer generate fields with same name as origin structure then skip the transformation
            setSkipStage(true);
            return structType;
        } else {
            return transSchema;
        }
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return getClass().getName() + serialVersionUID;
    }

    public SkippableTransformer setSkipStage(boolean isSkip) {
        skipStage = isSkip;
        return this;
    }

    public boolean getSkipStage() {
        return skipStage;
    }

}
