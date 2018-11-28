import examples.TestBase;
import featurePipeline.SkippableTransformer;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SkippableStagesTest extends TestBase {
    private static String masterUrl = "local";

    public SkippableStagesTest() {
        super(masterUrl);
    }

    @Test
    public void SkippableTransformer() {

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "Welcome to TutorialKart."),
                RowFactory.create(0.0, "Learn Spark at TutorialKart."),
                RowFactory.create(1.0, "Spark Mllib has TF-IDF.")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        // import data with the schema
        Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);
        String tokenizedColName = "tokenized";
        String hashTFColName = "hashTF";
        Pipeline noskipPipeline = new Pipeline();
        Pipeline skipPipelien = new Pipeline();

        Transformer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol(tokenizedColName);
        Transformer hashTF = new HashingTF().setInputCol(tokenizedColName).setOutputCol(hashTFColName);
        SkippableTransformer skipHashTF = new SkippableTransformer(hashTF).setSkipStage(true);

        noskipPipeline.setStages(new PipelineStage[]{tokenizer, hashTF});
        skipPipelien.setStages(new PipelineStage[]{tokenizer, skipHashTF});

        Dataset noSkipDataset = noskipPipeline.fit(sentenceData).transform(sentenceData);
        Dataset skippedDataset = skipPipelien.fit(sentenceData).transform(sentenceData);
        Assert.assertEquals(noSkipDataset.columns().length, 3);
        Assert.assertEquals(skippedDataset.columns().length, 2);
        noSkipDataset.show();
        skippedDataset.show();
    }
}
