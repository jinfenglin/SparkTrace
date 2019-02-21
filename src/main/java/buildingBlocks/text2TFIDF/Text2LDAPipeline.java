package buildingBlocks.text2TFIDF;

import buildingBlocks.preprocessor.EnglishPreprocess;
import buildingBlocks.vectorize.LDAPipeline;
import core.graphPipeline.basic.SGraph;

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

        SGraph p1 = EnglishPreprocess.getGraph("preprocess1"); // text - clean_tokens
        SGraph p2 = EnglishPreprocess.getGraph("preprocess2");
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