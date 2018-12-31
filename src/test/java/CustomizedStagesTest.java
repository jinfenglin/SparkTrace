import examples.TestBase;
import featurePipeline.NullRemoveWrapper.NullRemoverEstimatorSingleIO;
import featurePipeline.NullRemoveWrapper.NullRemoverModelSingleIO;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

/**
 *
 */
public class CustomizedStagesTest extends TestBase {
    private static String masterUrl = "local";

    public CustomizedStagesTest() {
        super(masterUrl);
    }

    @Test
    public void NullRemoverTransformerTest() {
        Dataset<Row> datasetWithNull = createDatasetWithNull();
        Tokenizer tk = new Tokenizer();

        NullRemoverModelSingleIO model = new NullRemoverModelSingleIO(tk);
        model.setInputCol("sentence");
        model.setOutputCol("tokens");
        model.transform(datasetWithNull).show();
    }
}
