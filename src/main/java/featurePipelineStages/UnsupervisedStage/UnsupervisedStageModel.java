package featurePipelineStages.UnsupervisedStage;


import featurePipelineStages.NullRemoveWrapper.NullRemoverModelSingleIO;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.logging.Logger;

/**
 *
 */
public class UnsupervisedStageModel extends Model<UnsupervisedStageModel> implements UnsupervisedStageParam {
    private static final long serialVersionUID = -1242019997300639915L;
    private Transformer innerTransformer;
    private StringArrayParam inputCols, outputCols;

    public UnsupervisedStageModel(Transformer innerTransformer) {
        this.innerTransformer = innerTransformer;
        inputCols = initInputCols();
        outputCols = initOutputCols();
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        transformSchema(dataset.schema());
        int length = getInputCols().length;
        for (int i = 0; i < length; i++) {
            String inputColName = getInputCols()[i];
            String outputColName = getOutputCols()[i];
            if (innerTransformer instanceof HasInputCol && innerTransformer instanceof HasOutputCol) {
                HasInputCol hasInput = (HasInputCol) innerTransformer;
                hasInput.set(hasInput.inputCol(), inputColName);
                HasOutputCol hasOutput = (HasOutputCol) innerTransformer;
                hasOutput.set(hasOutput.outputCol(), outputColName);
                dataset = new NullRemoverModelSingleIO(innerTransformer).transform(dataset);
            } else {
                Logger.getLogger(this.getClass().getName()).warning("Inner Transformer for Unsupervised Stage not implementing HasInputCol or HasOutputCol");
            }
        }
        return dataset.toDF();
    }

    @Override
    public StructType transformSchema(StructType structType) {
        int length = getInputCols().length;
        for (int i = 0; i < length; i++) {
            String inputColName = getInputCols()[i];
            String outputColName = getOutputCols()[i];
            if (innerTransformer instanceof HasInputCol && innerTransformer instanceof HasOutputCol) {
                HasInputCol hasInput = (HasInputCol) innerTransformer;
                hasInput.set(hasInput.inputCol(), inputColName);
                HasOutputCol hasOutput = (HasOutputCol) innerTransformer;
                hasOutput.set(hasOutput.outputCol(), outputColName);
                structType = innerTransformer.transformSchema(structType);
            } else {
                Logger.getLogger(this.getClass().getName()).warning("Inner Transformer for Unsupervised Stage not implementing HasInputCol or HasOutputCol");
            }
        }
        return structType;
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }

    @Override
    public UnsupervisedStageModel copy(ParamMap paramMap) {
        return new UnsupervisedStageModel(innerTransformer).setParent(parent());
    }

    @Override
    public StringArrayParam inputCols() {
        return this.inputCols;
    }

    @Override
    public StringArrayParam outputCols() {
        return this.outputCols;
    }
}
