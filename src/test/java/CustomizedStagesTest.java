import examples.TestBase;
import featurePipeline.NullRemoveWrapper.NullRemoverEstimatorSingleIO;
import featurePipeline.NullRemoveWrapper.NullRemoverModelSingleIO;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
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
    public void NullRemoverConfigRemoverTest() {
        Dataset<Row> datasetWithNull = createDatasetWithNull();

        Tokenizer tk = new Tokenizer("tokenizer");
        NullRemoverModelSingleIO model = new NullRemoverModelSingleIO(tk);
        model.setInputCol("sentence");
        model.setOutputCol("tokens");
        assert ((Tokenizer)model.getInnerStage()).getInputCol().equals("sentence");
        assert ((Tokenizer)model.getInnerStage()).getOutputCol().equals("tokens");
        datasetWithNull = model.transform(datasetWithNull);

        HashingTF htf = new HashingTF("htf");
        model = new NullRemoverModelSingleIO(htf);
        model.setInputCol("tokens");
        model.setOutputCol("htf");
        datasetWithNull = model.transform(datasetWithNull);

        IDF idf = new IDF("IDF");
        NullRemoverEstimatorSingleIO nullRemoverIDF = new NullRemoverEstimatorSingleIO(idf);
        nullRemoverIDF.setInputCol("htf");
        nullRemoverIDF.setOutputCol("TF-IDF");
        nullRemoverIDF.fit(datasetWithNull).transform(datasetWithNull).show(false);
    }

    @Test
    public void NullRemoverConfigInnerStageTest() {
        Dataset<Row> datasetWithNull = createDatasetWithNull();

        Tokenizer tk = new Tokenizer();
        tk.setInputCol("sentence");
        tk.setOutputCol("tokens");
        NullRemoverModelSingleIO model = new NullRemoverModelSingleIO(tk);
        datasetWithNull = model.transform(datasetWithNull);

        IDF idf = new IDF();
        idf.setInputCol("tokens");
        idf.setOutputCol("TF-IDF");
        NullRemoverEstimatorSingleIO nullRemoverIDF = new NullRemoverEstimatorSingleIO(idf);
        nullRemoverIDF.fit(datasetWithNull).transform(datasetWithNull).show();
    }
}
