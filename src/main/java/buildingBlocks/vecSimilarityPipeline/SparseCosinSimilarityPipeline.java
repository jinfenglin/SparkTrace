package buildingBlocks.vecSimilarityPipeline;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import featurePipelineStages.VecSimilarity.SparseVecSimilarity.SparseVecCosinSimilarityStage;

/**
 * This is a wrapper which contain only one node in a graph.
 */
public class SparseCosinSimilarityPipeline {
    public static String INPUT1 = "vec1", INPUT2 = "vec2";
    public static String OUTPUT = "cosin_sim";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph ddfGraph = new SGraph(graphName);
        ddfGraph.addInputField(INPUT1);
        ddfGraph.addInputField(INPUT2);
        ddfGraph.addOutputField(OUTPUT);

        String cosinIn1 = INPUT1;
        String cosinIn2 = INPUT2;
        String cosinOut = OUTPUT;
        SparseVecCosinSimilarityStage cosinSimilarityStage = new SparseVecCosinSimilarityStage();
        SNode cosinNode = new SNode(cosinSimilarityStage, "cosinSimilarity");
        cosinNode.addInputField(cosinIn1);
        cosinNode.addInputField(cosinIn2);
        cosinNode.addOutputField(cosinOut);

        ddfGraph.addNode(cosinNode);

        ddfGraph.connect(ddfGraph.sourceNode, INPUT1, cosinNode, cosinIn1);
        ddfGraph.connect(ddfGraph.sourceNode, INPUT2, cosinNode, cosinIn2);
        ddfGraph.connect(cosinNode, cosinOut, ddfGraph.sinkNode, OUTPUT);
        return ddfGraph;
    }
}
