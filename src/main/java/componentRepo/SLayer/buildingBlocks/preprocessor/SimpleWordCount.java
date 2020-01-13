package componentRepo.SLayer.buildingBlocks.preprocessor;

import componentRepo.SLayer.featurePipelineStages.SGraphColumnRemovalStage;
import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.HashingTF;

/**
 * Transfer the text into token count
 */
public class SimpleWordCount {
    public static String INPUT_TEXT_COL = "text";
    public static String OUTPUT_HTF = "htf";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(INPUT_TEXT_COL);
        graph.addOutputField(OUTPUT_HTF);

        SGraph cleanTokens = CleanTokens.getGraph("cleanToken");

        String htfIn = "tokens";
        String htfOut = "htf";
        HashingTF htf = new HashingTF("HashTF");
        SNode htfNode = new SNode(htf, "HTF");
        htfNode.addInputField("tokens");
        htfNode.addOutputField("htf");

        graph.addNode(cleanTokens);
        graph.addNode(htfNode);

        graph.connect(graph.sourceNode, INPUT_TEXT_COL, cleanTokens, CleanTokens.INPUT_TEXT_COL);
        graph.connect(cleanTokens, CleanTokens.OUTPUT_TOKENS, htfNode, htfIn);
        graph.connect(htfNode, htfOut, graph.sinkNode, OUTPUT_HTF);
        return graph;
    }
}
