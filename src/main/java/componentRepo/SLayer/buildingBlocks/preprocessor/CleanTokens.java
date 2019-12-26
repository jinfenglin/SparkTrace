package componentRepo.SLayer.buildingBlocks.preprocessor;

import componentRepo.SLayer.featurePipelineStages.cleanStage.CleanStage;
import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;

/**
 * A pre processing pipeline for english document. Tokenize, remove stop words, stem
 */
public class CleanTokens {
    public static String INPUT_TEXT_COL = "text";
    public static String OUTPUT_TOKENS = "cleanTokens";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(INPUT_TEXT_COL);
        graph.addOutputField(OUTPUT_TOKENS);

        CleanStage cs = new CleanStage();
        SNode csNode = new SNode(cs, "cleaner");
        csNode.addInputField("text");
        csNode.addOutputField("clean_text");

        Tokenizer tk = new Tokenizer();
        SNode tkNode = new SNode(tk, "tokenizer");
        tkNode.addInputField("clean_text");
        tkNode.addOutputField("tokens");

        StopWordsRemover remover = new StopWordsRemover();
        SNode removerNode = new SNode(remover, "stopWordRemover");
        removerNode.addInputField("tokens");
        removerNode.addOutputField("cleanTokens");

        graph.addNode(csNode);
        graph.addNode(tkNode);
        graph.addNode(removerNode);

        graph.connect(graph.sourceNode, "text", csNode, "text");
        graph.connect(csNode, "clean_text", tkNode, "clean_text");
        graph.connect(tkNode, "tokens", removerNode, "tokens");
        graph.connect(removerNode, "cleanTokens", graph.sinkNode, OUTPUT_TOKENS);
        return graph;
    }
}
