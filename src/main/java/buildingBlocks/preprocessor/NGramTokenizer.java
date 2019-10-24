package buildingBlocks.preprocessor;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import org.apache.spark.ml.feature.NGram;

/**
 *
 */
public class NGramTokenizer {
    public static String NGRAM_IN_TEXT = "text";
    public static String NGRAM_TOKENS = "ngram_htf";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(NGRAM_IN_TEXT);
        graph.addOutputField(NGRAM_TOKENS);

        SGraph prep1 = CleanTokens.getGraph("preprocess");

        String ngramIn = "tokens";
        String ngramOut = "ngram_tokens";
        NGram ngramStage = new NGram().setN(2);
        SNode ngramNode = new SNode(ngramStage, "ngram");
        ngramNode.addInputField(ngramIn);
        ngramNode.addOutputField(ngramOut);

        graph.addNode(prep1);
        graph.addNode(ngramNode);

        graph.connect(graph.sourceNode, NGRAM_IN_TEXT, prep1, CleanTokens.INPUT_TEXT_COL);
        graph.connect(prep1, CleanTokens.OUTPUT_TOKENS, ngramNode, ngramIn);
        graph.connect(ngramNode, ngramOut, graph.sinkNode, NGRAM_TOKENS);
        return graph;
    }
}
