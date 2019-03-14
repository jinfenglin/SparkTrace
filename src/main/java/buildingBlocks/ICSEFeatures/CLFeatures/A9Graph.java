package buildingBlocks.ICSEFeatures.CLFeatures;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.cloestLinkedCommit.CLTimeDiff;
import featurePipelineStages.cloestLinkedCommit.Overlap;

/**
 *
 */
public class A9Graph extends AbsCLFeature {
    private static final String FILES = "FILES";
    public static String A9 = "A9";

    public static SGraph getSGraph(String graphName, boolean findPrevious) throws Exception {
        SGraph graph = new SGraph(graphName);

        SNode findCLNode = createFindClosestLink(findPrevious);
        SNode a9Node = new SNode(new Overlap(), "Overlap");
        a9Node.addInputField(COMMIT_ID);
        a9Node.addInputField(FILES);
        a9Node.addInputField(CL);
        a9Node.addOutputField(A9);

        graph.addNode(findCLNode);
        graph.addNode(a9Node);

        connectFindClosestLink(graph, findCLNode);
        connectA9(graph, a9Node, findCLNode);
        return graph;
    }

    private static void connectA9(SGraph graph, SNode a9Node, SNode findCLNode) throws Exception {
        graph.connect(graph.sourceNode, COMMIT_ID, a9Node, COMMIT_ID);
        graph.connect(graph.sourceNode, FILES, a9Node, FILES);
        graph.connect(findCLNode, CL, a9Node, CL);
        graph.connect(a9Node, A9, graph.sinkNode, A9);
    }
}
