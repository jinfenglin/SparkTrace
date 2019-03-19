package buildingBlocks.ICSEFeatures.CLFeatures;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.cloestLinkedCommit.CLUser;
import org.apache.spark.ml.feature.StringIndexer;

public class A10Graph extends AbsCLFeature {
    private static String COMMIT_AUTHOR = "COMMIT_AUTHOR";
    public static String COMMIT_ID = "COMMIT_ID", COMMIT_DATE = "COMMIT_DATE", LINKED_COMMIT = "LINKED_COMMIT";
    private static String CLUSER = "CLUSER";
    public static String A10 = "A10";

    public static SGraph getGraph(String graphName, boolean findPrevious) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(COMMIT_ID);
        graph.addInputField(COMMIT_DATE);
        graph.addInputField(LINKED_COMMIT);
        graph.addInputField(COMMIT_AUTHOR);
        graph.addOutputField(A10);

        SNode findCLNode = createFindClosestLink(findPrevious);

        SNode a10Node = new SNode(new CLUser(), "CLUser");
        a10Node.addInputField(COMMIT_ID);
        a10Node.addInputField(COMMIT_AUTHOR);
        a10Node.addInputField(CL);
        a10Node.addOutputField(CLUSER);

        SNode indexerNode = new SNode(new StringIndexer().setHandleInvalid("keep"), "Indexer");
        indexerNode.addInputField(CLUSER);
        indexerNode.addOutputField(A10);

        graph.addNode(findCLNode);
        graph.addNode(a10Node);
        graph.addNode(indexerNode);

        connectFindClosestLink(graph, findCLNode);
        graph.connect(graph.sourceNode, COMMIT_ID, a10Node, COMMIT_ID);
        graph.connect(graph.sourceNode, COMMIT_AUTHOR, a10Node, COMMIT_AUTHOR);
        graph.connect(findCLNode, CL, a10Node, CL);
        graph.connect(a10Node, CLUSER, indexerNode, CLUSER);
        graph.connect(indexerNode, A10, graph.sinkNode, A10);

        return graph;
    }


}
