package buildingBlocks.ICSEFeatures;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import featurePipelineStages.sameColumnStage.SameAuthorStage;

/**
 *
 */
public class LCA3 {
    public static String AUTHOR1 = "AUTHOR1", AUTHOR2 = "AUTHOR2";
    public static String SAME_AUTHOR = "SAME_AUTHOR";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(AUTHOR1);
        graph.addInputField(AUTHOR2);
        graph.addOutputField(SAME_AUTHOR);

        SameAuthorStage stage = new SameAuthorStage();
        SNode sameAuthorNode = new SNode(stage, "SameAuthor");
        sameAuthorNode.addInputField(AUTHOR1);
        sameAuthorNode.addInputField(AUTHOR2);
        sameAuthorNode.addOutputField(SAME_AUTHOR);

        graph.addNode(sameAuthorNode);

        graph.connect(graph.sourceNode, AUTHOR1, sameAuthorNode, AUTHOR1);
        graph.connect(graph.sourceNode, AUTHOR2, sameAuthorNode, AUTHOR2);
        graph.connect(sameAuthorNode, SAME_AUTHOR, graph.sinkNode, SAME_AUTHOR);

        return graph;
    }
}
