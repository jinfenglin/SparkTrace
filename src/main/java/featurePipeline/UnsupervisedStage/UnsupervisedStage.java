package featurePipeline.UnsupervisedStage;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

/**
 * A wrapper for any pipeline stage. This stage collect multiple columns as a single column to generate a new dataset
 * thus the inner stage can fit on this dataset. This is design for unsupervised learning stage which may use both
 * source artifact and target artifact. The trained model will then transform each of the column separately and output
 * them back to origin data frame
 */
public class UnsupervisedStage extends Estimator<UnsupervisedStageModel> implements UnsupervisedStageParam {
    private static final long serialVersionUID = 7000401544310981006L;
    private Estimator innerEstimator;
    private DataType columnDataType;
    private StringArrayParam inputCols, outputCols;

    public UnsupervisedStage(Estimator innerEstimator) {
        this.innerEstimator = innerEstimator;
        inputCols = initInputCols();
        outputCols = initOutputCols();
    }


    /**
     * Ensure the input columns have same dataType.
     *
     * @param structType
     * @return
     */
    @Override
    public StructType transformSchema(StructType structType) {
        Set<String> inputCols = new HashSet<>();
        inputCols.addAll(Arrays.asList(getInputCols()));
        StructField[] fields = structType.fields();
        List<StructField> columnFields = Arrays.stream(fields).filter(x -> inputCols.contains(x.name())).collect(Collectors.toList());
        columnDataType = columnFields.get(0).dataType();
        for (StructField field : columnFields) {
            assert field.dataType().equals(columnDataType);
        }

        int length = getInputCols().length;
        for (int i = 0; i < length; i++) {
            String inputColName = getInputCols()[i];
            String outputColName = getOutputCols()[i];
            if (innerEstimator instanceof HasInputCol && innerEstimator instanceof HasOutputCol) {
                HasInputCol hasInput = (HasInputCol) innerEstimator;
                hasInput.set(hasInput.inputCol(), inputColName);
                HasOutputCol hasOutput = (HasOutputCol) innerEstimator;
                hasOutput.set(hasOutput.outputCol(), outputColName);
                structType = innerEstimator.transformSchema(structType);
            } else {
                Logger.getLogger(this.getClass().getName()).warning("Inner Transformer for Unsupervised Stage not implementing HasInputCol or HasOutputCol");
            }
        }
        return structType;
    }

    @Override
    public UnsupervisedStageModel fit(Dataset<?> dataset) {
        //Create an empty temporal dataframe which have one column
        transformSchema(dataset.schema());
        String mixedInputCol = "mixedInputCol";
        String mixedOutputCol = "mixedOutputCol";
        StructField field = DataTypes.createStructField(mixedInputCol, columnDataType, false);
        StructType st = new StructType(new StructField[]{field});
        Dataset<Row> trainingData = dataset.sparkSession().createDataFrame(new ArrayList<>(), st);

        for (String colName : getInputCols()) {
            Dataset<Row> columnData = dataset.select(colName).where(col(colName).isNotNull());
            trainingData = trainingData.union(columnData);
        }

        if (innerEstimator instanceof HasInputCol && innerEstimator instanceof HasOutputCol) {
            HasInputCol hasInput = (HasInputCol) innerEstimator;
            hasInput.set(hasInput.inputCol(), mixedInputCol);
            HasOutputCol hasOutput = (HasOutputCol) innerEstimator;
            hasOutput.set(hasOutput.outputCol(), mixedOutputCol);
            Transformer innerTransformer = innerEstimator.fit(trainingData);
            UnsupervisedStageModel model = new UnsupervisedStageModel(innerTransformer).setParent(this);
            model.setInputCols(getInputCols());
            model.setOutputCols(getOutputCols());
            return model;
        } else {
            Logger.getLogger(this.getClass().getName()).warning("Inner Transformer for Unsupervised Stage not implementing HasInputCol or HasOutputCol");
            return null;
        }
    }

    @Override
    public Estimator<UnsupervisedStageModel> copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
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
