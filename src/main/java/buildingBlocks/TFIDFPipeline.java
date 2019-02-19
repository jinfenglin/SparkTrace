package buildingBlocks;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipeline.NullRemoveWrapper.NullRemoverModelSingleIO;
import featurePipeline.UnsupervisedStage.UnsupervisedStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;

/**
 *
 */
public class TFIDFPipeline {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField("tokens1");
        graph.addInputField("tokens2");
        graph.addOutputField("tf-idf1");
        graph.addOutputField("tf-idf2");

        HashingTF htf1 = new HashingTF();
        SNode htfNode1 = new SNode(new NullRemoverModelSingleIO(htf1), "htf1");
        htfNode1.addInputField("tokens");
        htfNode1.addOutputField("htf");

        HashingTF htf2 = new HashingTF();
        SNode htfNode2 = new SNode(new NullRemoverModelSingleIO(htf2), "htf2");
        htfNode2.addInputField("tokens");
        htfNode2.addOutputField("htf");

        IDF idf = new IDF();
        UnsupervisedStage un_idf = new UnsupervisedStage(idf);
        SNode idfNode = new SNode(un_idf, "shared_IDF");
        idfNode.addInputField("vec1");
        idfNode.addInputField("vec2");
        idfNode.addOutputField("idf1");
        idfNode.addOutputField("idf2");

        graph.addNode(htfNode1);
        graph.addNode(htfNode2);
        graph.addNode(idfNode);


        graph.connect(graph.sourceNode, "tokens1", htfNode1, "tokens");
        graph.connect(htfNode1, "htf", idfNode, "vec1");
        graph.connect(idfNode, "idf1", graph.sinkNode, "tf-idf1");


        graph.connect(graph.sourceNode, "tokens2", htfNode2, "tokens");
        graph.connect(htfNode2, "htf", idfNode, "vec2");
        graph.connect(idfNode, "idf2", graph.sinkNode, "tf-idf2");

        return graph;

    }
}
