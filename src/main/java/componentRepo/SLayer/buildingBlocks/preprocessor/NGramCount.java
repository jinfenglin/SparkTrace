package componentRepo.SLayer.buildingBlocks.preprocessor;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import org.apache.spark.ml.feature.HashingTF;

/**
 *
 */
public class NGramCount {
    public static String INPUT_TEXT_COL = "text";
    public static String OUTPUT_HTF = "ngram_htf";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(INPUT_TEXT_COL);
        graph.addOutputField(OUTPUT_HTF);

        SGraph ngramTokenizer = NGramTokenizer.getGraph("cleanToken");

        String htfIn = "tokens";
        String htfOut = "htf";
        HashingTF htf = new HashingTF("HashTF");
        SNode htfNode = new SNode(htf, "HTF");
        htfNode.addInputField("tokens");
        htfNode.addOutputField("htf");

        graph.addNode(ngramTokenizer);
        graph.addNode(htfNode);

        graph.connect(graph.sourceNode, INPUT_TEXT_COL, ngramTokenizer, NGramTokenizer.NGRAM_IN_TEXT);
        graph.connect(ngramTokenizer, NGramTokenizer.NGRAM_TOKENS, htfNode, htfIn);
        graph.connect(htfNode, htfOut, graph.sinkNode, OUTPUT_HTF);
        return graph;
    }
}
