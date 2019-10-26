package componentRepo.SLayer.buildingBlocks.unsupervisedLearn;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import componentRepo.SLayer.featurePipelineStages.LDAWithIO.LDAWithIO;

/**
 *
 */
public class LDAGraphPipeline {
    public static String INPUT1 = "tokens1", INPUT2 = "tokens2";
    public static String OUTPUT1 = "topics1", OUTPUT2 = "topics2";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph ddfGraph = new SGraph(graphName);
        ddfGraph.addInputField(INPUT1);
        ddfGraph.addInputField(INPUT2);
        ddfGraph.addOutputField(OUTPUT1);
        ddfGraph.addOutputField(OUTPUT2);

        LDAWithIO lda = new LDAWithIO();
        SNode ldaNode = new SNode(lda, "LDA");
        ldaNode.addInputField(INPUT1);
        ldaNode.addInputField(INPUT2);
        ldaNode.addOutputField(OUTPUT1);
        ldaNode.addOutputField(OUTPUT2);

        ddfGraph.addNode(ldaNode);

        ddfGraph.connect(ddfGraph.sourceNode, INPUT1, ldaNode, INPUT1);
        ddfGraph.connect(ddfGraph.sourceNode, INPUT2, ldaNode, INPUT2);
        ddfGraph.connect(ldaNode, OUTPUT1, ddfGraph.sinkNode, OUTPUT1);
        ddfGraph.connect(ldaNode, OUTPUT2, ddfGraph.sinkNode, OUTPUT2);
        return ddfGraph;
    }
}
