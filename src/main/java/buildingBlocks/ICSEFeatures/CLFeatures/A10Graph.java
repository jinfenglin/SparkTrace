package buildingBlocks.ICSEFeatures.CLFeatures;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.cloestLinkedCommit.CLUser;

public class A10Graph extends AbsCLFeature {
    private static String COMMIT_AUTHOR = "COMMIT_AUTHOR";
    public static String A10 = "A10";

    public static SGraph getGraph(String graphName, boolean findPrevious) throws Exception {
        SGraph graph = new SGraph(graphName);
        SNode findCLNode = createFindClosestLink(findPrevious);

        SNode a10Node = new SNode(new CLUser());
        a10Node.addInputField(COMMIT_ID);
        a10Node.addInputField(COMMIT_AUTHOR);
        a10Node.addInputField(CL);
        a10Node.addOutputField(A10);

        graph.addNode(findCLNode);
        graph.addNode(a10Node);

        connectFindClosestLink(graph, findCLNode);
        connectA10(graph, a10Node, findCLNode);
        return graph;
    }

    private static void connectA10(SGraph graph, SNode a10Node, SNode findCLNode) throws Exception {
        graph.connect(graph.sourceNode, COMMIT_ID, a10Node, COMMIT_ID);
        graph.connect(graph.sourceNode, COMMIT_DATE, a10Node, COMMIT_DATE);
        graph.connect(findCLNode, CL, a10Node, CL);
        graph.connect(a10Node, A10, graph.sinkNode, A10);
    }


}
