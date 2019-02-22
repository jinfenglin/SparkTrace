package buildingBlocks.vecSimilarityPipeline;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.VecSimilarity.DenseVecSimilarity.DenseVecSimilarity;
/**
 *
 */
public class DenseCosinSimilarityPipeline {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph ddfGraph = new SGraph(graphName);
        ddfGraph.addInputField("vec1");
        ddfGraph.addInputField("vec2");
        ddfGraph.addOutputField("cosin_sim");

        DenseVecSimilarity cosinSimilarityStage = new DenseVecSimilarity();
        SNode cosinNode = new SNode(cosinSimilarityStage, "cosinSimilarity");
        cosinNode.addInputField("vec1");
        cosinNode.addInputField("vec2");
        cosinNode.addOutputField("cosin_sim");

        ddfGraph.addNode(cosinNode);

        ddfGraph.connect(ddfGraph.sourceNode, "vec1", cosinNode, "vec1");
        ddfGraph.connect(ddfGraph.sourceNode, "vec2", cosinNode, "vec2");
        ddfGraph.connect(cosinNode, "cosin_sim", ddfGraph.sinkNode, "cosin_sim");
        return ddfGraph;
    }
}
