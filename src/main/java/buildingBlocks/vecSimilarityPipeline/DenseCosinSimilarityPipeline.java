package buildingBlocks.vecSimilarityPipeline;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import featurePipelineStages.VecSimilarity.DenseVecSimilarity.DenseVecSimilarity;

/**
 *
 */
public class DenseCosinSimilarityPipeline {
    public static String INPUT1 = "vec1", INPUT2 = "vec2", OUTPUT = "cosin_sim";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph ddfGraph = new SGraph(graphName);
        ddfGraph.addInputField(INPUT1);
        ddfGraph.addInputField(INPUT2);
        ddfGraph.addOutputField(OUTPUT);

        DenseVecSimilarity cosinSimilarityStage = new DenseVecSimilarity();
        SNode cosinNode = new SNode(cosinSimilarityStage, "cosinSimilarity");
        cosinNode.addInputField(INPUT1);
        cosinNode.addInputField(INPUT2);
        cosinNode.addOutputField(OUTPUT);

        ddfGraph.addNode(cosinNode);

        ddfGraph.connect(ddfGraph.sourceNode, INPUT1, cosinNode, INPUT1);
        ddfGraph.connect(ddfGraph.sourceNode, INPUT2, cosinNode, INPUT2);
        ddfGraph.connect(cosinNode, OUTPUT, ddfGraph.sinkNode, OUTPUT);
        return ddfGraph;
    }
}
