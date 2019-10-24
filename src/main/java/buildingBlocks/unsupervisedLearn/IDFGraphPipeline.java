package buildingBlocks.unsupervisedLearn;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import org.apache.spark.ml.feature.IDF;

/**
 *
 */
public class IDFGraphPipeline {
    public static String INPUT1 = "text1", INPUT2 = "text2";
    public static String OUTPUT1 = "idfVec1", OUTPUT2 = "idfVec2";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph ddfGraph = new SGraph(graphName);
        ddfGraph.addInputField(INPUT1);
        ddfGraph.addInputField(INPUT2);
        ddfGraph.addOutputField(OUTPUT1);
        ddfGraph.addOutputField(OUTPUT2);

        IDF idf = new IDF();
        SNode idfNode = new SNode(idf, "IDF");
        idfNode.addInputField("text1");
        idfNode.addInputField("text2");
        idfNode.addOutputField("idfVec1");
        idfNode.addOutputField("idfVec2");

        ddfGraph.addNode(idfNode);

        ddfGraph.connect(ddfGraph.sourceNode, INPUT1, idfNode, "text1");
        ddfGraph.connect(ddfGraph.sourceNode, INPUT2, idfNode, "text2");
        ddfGraph.connect(idfNode, "idfVec1", ddfGraph.sinkNode, OUTPUT1);
        ddfGraph.connect(idfNode, "idfVec2", ddfGraph.sinkNode, OUTPUT2);
        return ddfGraph;
    }
}
