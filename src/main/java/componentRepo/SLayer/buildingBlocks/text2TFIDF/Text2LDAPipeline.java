package componentRepo.SLayer.buildingBlocks.text2TFIDF;

import componentRepo.SLayer.buildingBlocks.preprocessor.CleanTokens;
import componentRepo.SLayer.buildingBlocks.vectorize.LDAPipeline;
import core.graphPipeline.SLayer.SGraph;

/**
 *
 */
public class Text2LDAPipeline {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField("text1");
        graph.addInputField("text2");
        graph.addOutputField("topics1");
        graph.addOutputField("topics2");

        SGraph p1 = CleanTokens.getGraph("preprocess1"); // text - clean_tokens
        SGraph p2 = CleanTokens.getGraph("preprocess2");
        SGraph ldaPipe = LDAPipeline.getGraph("ldaPipeline"); //tokens1

        graph.addNode(p1);
        graph.addNode(p2);
        graph.addNode(ldaPipe);

        graph.connect(graph.sourceNode, "text1", p1, "text");
        graph.connect(p1, "cleanTokens", ldaPipe, "tokens1");
        graph.connect(ldaPipe, "topics1", graph.sinkNode, "topics1");

        graph.connect(graph.sourceNode, "text2", p2, "text");
        graph.connect(p2, "cleanTokens", ldaPipe, "tokens2");
        graph.connect(ldaPipe, "topics2", graph.sinkNode, "topics2");
        return graph;
    }
}
