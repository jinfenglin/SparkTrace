import buildingBlocks.EnglishPreprocess;
import buildingBlocks.TFIDFPipeline;
import core.graphPipeline.basic.SGraph;
import examples.TestBase;
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
    public void TFIDFPipelineTest() throws Exception {
        SGraph graph = new TFIDFPipeline().getGraph("tfidf");
        Map config = new HashMap<>();
        config.put("text1", "text1");
        config.put("text2", "text2");
        graph.setConfig(config);
        graph.toPipeline().fit(getMultiSentenceRowData()).transform(getMultiSentenceRowData()).show();
    }
}
