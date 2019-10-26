package componentRepo.SLayer.buildingBlocks.ICSEFeatures;

import componentRepo.SLayer.buildingBlocks.ICSEFeatures.CLFeatures.A10Graph;
import componentRepo.SLayer.buildingBlocks.ICSEFeatures.CLFeatures.A8Graph;
import componentRepo.SLayer.buildingBlocks.ICSEFeatures.CLFeatures.A9Graph;
import core.graphPipeline.SLayer.SGraph;

/**
 *
 */
public class LCA8ToA10 {
    public static String LINKED_COMMIT = "LINKED_COMMIT", COMMIT_ID = "COMMIT_ID",
            COMMIT_AUTHOR = "COMMIT_AUTHOR", COMMIT_DATE = "COMMIT_DATE", FILES = "FILES";
    public static String CL = "CL";
    public static String A8 = "A8", A9 = "A9", A10 = "A10";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(LINKED_COMMIT);
        graph.addInputField(COMMIT_ID);
        graph.addInputField(COMMIT_AUTHOR);
        graph.addInputField(COMMIT_DATE);
        graph.addInputField(FILES);
        graph.addOutputField(A8);
        graph.addOutputField(A9);
        graph.addOutputField(A10);

        SGraph a8Graph = A8Graph.getSGraph("A8Graph", true);
        SGraph a9Graph = A9Graph.getSGraph("A9Graph", true);
        SGraph a10Graph = A10Graph.getGraph("A10Graph", true);


        graph.addNode(a8Graph);
        graph.addNode(a9Graph);
        graph.addNode(a10Graph);

        connectSource(graph, a8Graph);
        connectSource(graph, a9Graph);
        connectSource(graph, a10Graph);

        graph.connect(graph.sourceNode, COMMIT_AUTHOR, a10Graph, COMMIT_AUTHOR);
        graph.connect(graph.sourceNode, FILES, a9Graph, FILES);


        graph.connect(a8Graph, A8, graph.sinkNode, A8);
        graph.connect(a9Graph, A9, graph.sinkNode, A9);
        graph.connect(a10Graph, A10, graph.sinkNode, A10);

        return graph;
    }

    private static void connectSource(SGraph graph, SGraph featureGraph) throws Exception {
        graph.connect(graph.sourceNode, COMMIT_ID, featureGraph, COMMIT_ID);
        graph.connect(graph.sourceNode, COMMIT_DATE, featureGraph, COMMIT_DATE);
        graph.connect(graph.sourceNode, LINKED_COMMIT, featureGraph, LINKED_COMMIT);
    }
}
