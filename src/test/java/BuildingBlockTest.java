import buildingBlocks.preprocessor.EnglishPreprocess;
import buildingBlocks.preprocessor.NGramPreprocessPipeline;
import buildingBlocks.text2TFIDF.Text2LDAPipeline;
import buildingBlocks.text2TFIDF.Text2NGramTFIDFPipeline;
import buildingBlocks.text2TFIDF.Text2TFIDFPipeline;
import core.graphPipeline.basic.SGraph;
import examples.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class BuildingBlockTest extends TestBase {
    private static final String masterUrl = "local";

    public BuildingBlockTest() {
        super(masterUrl);
    }

    @Test
    public void EnglishPreprocessingPipelineTest() throws Exception {
        SGraph graph = EnglishPreprocess.getGraph("");
        Map config = new HashMap<>();
        config.put("text", "sentence");
        graph.setConfig(config);
        graph.toPipeline().fit(getSentenceLabelDataset()).transform(getSentenceLabelDataset()).show(false);
    }

    @Test
    public void NgramPipeline() throws Exception {
        SGraph graph = NGramPreprocessPipeline.getGraph("ngram");
        Map config = new HashMap<>();
        config.put("text", "sentence");
        graph.setConfig(config);
        Dataset<Row> dataset = graph.toPipeline().fit(getSentenceLabelDataset()).transform(getSentenceLabelDataset());
        dataset.show();
//        HashingTF htf = new HashingTF();
//        htf.setInputCol(dataset.columns()[1]);
//        dataset = htf.transform(dataset);
//        IDF idf = new IDF();
//        idf.setInputCol(htf.getOutputCol());
//        idf.fit(dataset).transform(dataset).show();
    }

    @Test
    public void TFIDFPipelineTest() throws Exception {
        SGraph graph = Text2TFIDFPipeline.getGraph("tfidf");
        Map config = new HashMap<>();
        config.put("text1", "text1");
        config.put("text2", "text2");
        config.put("tf-idf1", "tf-idf1");
        config.put("tf-idf2", "tf-idf2");
        graph.setConfig(config);
        graph.toPipeline().fit(getMultiSentenceRowData()).transform(getMultiSentenceRowData()).show();
    }

    @Test
    public void NgramTFIDF() throws Exception {
        SGraph graph = Text2NGramTFIDFPipeline.getGraph("tfidf");
        Map config = new HashMap<>();
        config.put("text1", "text1");
        config.put("text2", "text2");
        config.put("ngram-tf-idf1", "ngram-tf-idf1");
        config.put("ngram-tf-idf2", "ngram-tf-idf2");
        graph.setConfig(config);
        graph.toPipeline().fit(getMultiSentenceRowData()).transform(getMultiSentenceRowData()).show();
    }

    @Test
    public void LDAPipelineTest() throws Exception {
        SGraph graph = Text2LDAPipeline.getGraph("ldaTest");
        Map config = new HashMap<>();
        config.put("text1", "text1");
        config.put("text2", "text2");
        graph.setConfig(config);
        graph.toPipeline().fit(getMultiSentenceRowData()).transform(getMultiSentenceRowData()).show(false);
    }

}
