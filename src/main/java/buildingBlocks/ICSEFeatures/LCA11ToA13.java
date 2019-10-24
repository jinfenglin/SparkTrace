package buildingBlocks.ICSEFeatures;

import buildingBlocks.ICSEFeatures.CLFeatures.A10Graph;
import buildingBlocks.ICSEFeatures.CLFeatures.A8Graph;
import buildingBlocks.ICSEFeatures.CLFeatures.A9Graph;
import core.graphPipeline.SLayer.SGraph;

/**
 *
 */
public class LCA11ToA13 {
    public static String LINKED_COMMIT = "LINKED_COMMIT", COMMIT_ID = "COMMIT_ID",
            COMMIT_AUTHOR = "COMMIT_AUTHOR", COMMIT_DATE = "COMMIT_DATE", FILES = "FILES";
    public static String CL = "CL";
    public static String A11 = "A11", A12 = "A12", A13 = "A13";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(LINKED_COMMIT);
        graph.addInputField(COMMIT_ID);
        graph.addInputField(COMMIT_AUTHOR);
        graph.addInputField(COMMIT_DATE);
        graph.addInputField(FILES);
        graph.addOutputField(A11);
        graph.addOutputField(A12);
        graph.addOutputField(A13);

        SGraph a11Graph = A8Graph.getSGraph("A11Graph", false);
        SGraph a12Graph = A9Graph.getSGraph("A12Graph", false);
        SGraph a13Graph = A10Graph.getGraph("A13Graph", false);


        graph.addNode(a11Graph);
        graph.addNode(a12Graph);
        graph.addNode(a13Graph);

        connectSource(graph, a11Graph);
        connectSource(graph, a12Graph);
        connectSource(graph, a13Graph);
        graph.connect(graph.sourceNode, COMMIT_AUTHOR, a13Graph, COMMIT_AUTHOR);
        graph.connect(graph.sourceNode, FILES, a12Graph, FILES);

        graph.connect(a11Graph, A8Graph.A8, graph.sinkNode, A11);
        graph.connect(a12Graph, A9Graph.A9, graph.sinkNode, A12);
        graph.connect(a13Graph, A10Graph.A10, graph.sinkNode, A13);

        return graph;
    }

    private static void connectSource(SGraph graph, SGraph featureGraph) throws Exception {
        graph.connect(graph.sourceNode, COMMIT_ID, featureGraph, COMMIT_ID);
        graph.connect(graph.sourceNode, COMMIT_DATE, featureGraph, COMMIT_DATE);
        graph.connect(graph.sourceNode, LINKED_COMMIT, featureGraph, LINKED_COMMIT);
    }
}
