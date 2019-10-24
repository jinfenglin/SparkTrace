package buildingBlocks.ICSEFeatures.CLFeatures;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import featurePipelineStages.cloestLinkedCommit.CLTimeDiff;

public class A8Graph extends AbsCLFeature {
    public static String A8 = "A8";
    public static String COMMIT_ID = "COMMIT_ID", COMMIT_DATE = "COMMIT_DATE", LINKED_COMMIT = "LINKED_COMMIT";

    public static SGraph getSGraph(String graphName, boolean findPrevious) throws Exception {
        SGraph sGraph = new SGraph(graphName);
        sGraph.addInputField(COMMIT_ID);
        sGraph.addInputField(COMMIT_DATE);
        sGraph.addInputField(LINKED_COMMIT);
        sGraph.addOutputField(A8);

        SNode findCLNode = createFindClosestLink(findPrevious);
        SNode A8Node = new SNode(new CLTimeDiff(), "CLTimeDiff");
        A8Node.addInputField(COMMIT_ID);
        A8Node.addInputField(COMMIT_DATE);
        A8Node.addInputField(CL);
        A8Node.addOutputField(A8);

        sGraph.addNode(findCLNode);
        sGraph.addNode(A8Node);

        connectFindClosestLink(sGraph, findCLNode);
        connectA8(sGraph, A8Node, findCLNode);
        return sGraph;
    }

    private static void connectA8(SGraph graph, SNode a8Node, SNode findCLNode) throws Exception {
        graph.connect(graph.sourceNode, COMMIT_ID, a8Node, COMMIT_ID);
        graph.connect(graph.sourceNode, COMMIT_DATE, a8Node, COMMIT_DATE);
        graph.connect(findCLNode, CL, a8Node, CL);
        graph.connect(a8Node, A8, graph.sinkNode, A8);
    }
}
