package buildingBlocks.vectorize;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import org.apache.spark.ml.feature.HashingTF;

/**
 *
 */
public class LDAPipeline {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField("tokens1");
        graph.addInputField("tokens2");
        graph.addOutputField("topics1");
        graph.addOutputField("topics2");

        HashingTF htf1 = new HashingTF();
        SNode htfNode1 = new SNode(htf1, "htf1");
        htfNode1.addInputField("tokens");
        htfNode1.addOutputField("htf");

        HashingTF htf2 = new HashingTF();
        SNode htfNode2 = new SNode(htf2, "htf2");
        htfNode2.addInputField("tokens");
        htfNode2.addOutputField("htf");

//        LDA lda = new LDAWithIO().setK(10).setMaxIter(5).setOptimizer("em");
//        UnsupervisedStage un_lda = new UnsupervisedStage(lda);
//        SNode ldaNode = new SNode(un_lda, "shared_LDA");
//        ldaNode.addInputField("vec1");
//        ldaNode.addInputField("vec2");
//        ldaNode.addOutputField("topics1");
//        ldaNode.addOutputField("topics2");
//
//        graph.addNode(htfNode1);
//        graph.addNode(htfNode2);
//        graph.addNode(ldaNode);
//
//        graph.connect(graph.sourceNode, "tokens1", htfNode1, "tokens");
//        graph.connect(htfNode1, "htf", ldaNode, "vec1");
//        graph.connect(ldaNode, "topics1", graph.sinkNode, "topics1");
//
//        graph.connect(graph.sourceNode, "tokens2", htfNode2, "tokens");
//        graph.connect(htfNode2, "htf", ldaNode, "vec2");
//        graph.connect(ldaNode, "topics2", graph.sinkNode, "topics2");
//
        return graph;

    }
}
