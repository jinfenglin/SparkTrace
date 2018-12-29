package featurePipeline.UnsupervisedStage;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCols;
import org.apache.spark.ml.util.SchemaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;
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


    @Override
    public StructType transformSchema(StructType structType) {
        Set<String> inputCols = new HashSet<>();
        inputCols.addAll(Arrays.asList(getInputCols()));
        StructField[] fields = structType.fields();
        List<StructField> columnFields = Arrays.stream(fields).filter(x -> inputCols.contains(x.name())).collect(Collectors.toList());
        columnDataType = columnFields.get(0).dataType();
        for (StructField field : columnFields) {
            field.dataType().equals(columnDataType);
        }
        return structType;
    }

    @Override
    public UnsupervisedStageModel fit(Dataset<?> dataset) {
        //Create an empty temporal dataframe which have one column
        String mixedInputCol = "mixedInputCol";
        StructField field = DataTypes.createStructField(mixedInputCol, columnDataType, false);
        StructType st = new StructType(new StructField[]{field});
        Dataset<Row> trainingData = dataset.sparkSession().createDataFrame(new ArrayList<>(), st);

        for (String colName : getInputCols()) {
            Dataset<Row> columnData = dataset.select(colName).filter(Row::anyNull);
            trainingData = trainingData.union(columnData);
        }
        Transformer innerTransformer = innerEstimator.fit(trainingData);
        UnsupervisedStageModel model = new UnsupervisedStageModel(innerTransformer);
        model.setInputCols(getInputCols());
        model.setOutputCols(getOutputCols());
        return model;
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
