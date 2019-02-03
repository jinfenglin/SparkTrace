import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import examples.TestBase;
import featurePipeline.DummyStage;
import featurePipeline.SGraphIOStage;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static guru.nidi.graphviz.model.Factory.*;

/**
 *
 */
public class OptimizationTest extends TestBase {
    private static String masterUrl = "local";

    public OptimizationTest() {
        super(masterUrl);
    }

    @Test
    public void multiTaskOptimizationTest() {
        //TOOD add test where 2 VSMTask is created in parallel
    }

    @Test
    public void NoSubGraphOptimizationTest() throws Exception {
        Dataset<Row> dataset = getSentenceLabelDataset();
        SGraph graph = new SGraph("NoSubGraphOptimization");
        graph.addInputField("sentence");
        graph.addOutputField("token1");
        graph.addOutputField("token2");

        Tokenizer tk1 = new Tokenizer();
        SNode tkNode1 = new SNode(tk1, "tokenizer1");
        tkNode1.addInputField("text");
        tkNode1.addOutputField("tokens1");

        Tokenizer tk2 = new Tokenizer();
        SNode tkNode2 = new SNode(tk2, "tokenizer2");
        tkNode2.addInputField("text");
        tkNode2.addOutputField("tokens2");

        graph.addNode(tkNode1);
        graph.addNode(tkNode2);
        graph.connect(graph.sourceNode, "sentence", tkNode1, "text");
        graph.connect(tkNode1, "tokens1", graph.sinkNode, "token1");
        graph.connect(graph.sourceNode, "sentence", tkNode2, "text");
        graph.connect(tkNode2, "tokens2", graph.sinkNode, "token2");
        graph.optimize(graph);
        Dataset<Row> result = graph.toPipeline().fit(dataset).transform(dataset);
        result.show();
    }

    /**
     * Create two tokenizer nodes, one has a dummy preceding node (ensure the other tokenizer have lower topo index),
     * and a consumer which utilize the token. Verify the optimization function work properly.
     */
    @Test
    public void RemoveNodeHasDependency() throws Exception {
        Dataset<Row> dataset = getSentenceLabelDataset();
        SGraph graph = new SGraph("NoSubGraphOptimization");
        graph.addInputField("sentence");
        graph.addOutputField("token");
        graph.addOutputField("hashTF");

        Tokenizer tk1 = new Tokenizer();
        SNode tkNode1 = new SNode(tk1, "tokenizer1");
        tkNode1.addInputField("text");
        tkNode1.addOutputField("tokens1");


        Tokenizer tk2 = new Tokenizer();
        SNode tkNode2 = new SNode(tk2, "tokenizer2");
        tkNode2.addInputField("text");
        tkNode2.addOutputField("tokens2");

        HashingTF hashingTF = new HashingTF();
        SNode hashTFNode = new SNode(hashingTF, "hashTF");
        hashTFNode.addInputField("tokenInput");
        hashTFNode.addOutputField("TF");

        graph.addNode(tkNode1);
        graph.addNode(tkNode2);
        graph.addNode(hashTFNode);

        graph.connect(graph.sourceNode, "sentence", tkNode1, "text");
        graph.connect(tkNode1, "tokens1", graph.sinkNode, "token");

        graph.connect(graph.sourceNode, "sentence", tkNode2, "text");
        graph.connect(tkNode2, "tokens2", hashTFNode, "tokenInput");
        graph.connect(hashTFNode, "TF", graph.sinkNode, "hashTF");
        graph.showGraph("RemoveNodeHasDependency_before_optimize");
        graph.optimize(graph);
        Dataset<Row> result = graph.toPipeline().fit(dataset).transform(dataset);
        graph.showGraph("RemoveNodeHasDependency_after_optimize");
        result.show();
    }

    @Test
    public void subGraphOptimization() throws Exception {
        Dataset<Row> dataset = getSentenceLabelDataset();

        SGraph subGraph = new SGraph("subGraph");
        subGraph.addInputField("text");
        subGraph.addOutputField("TF");

        Tokenizer tk2 = new Tokenizer();
        SNode tkNode2 = new SNode(tk2, "tokenizer2");
        tkNode2.addInputField("text");
        tkNode2.addOutputField("tokens2");

        HashingTF hashingTF = new HashingTF();
        SNode hashTFNode = new SNode(hashingTF, "hashTF");
        hashTFNode.addInputField("tokenInput");
        hashTFNode.addOutputField("TF");

        subGraph.addNode(tkNode2);
        subGraph.addNode(hashTFNode);

        subGraph.connect(subGraph.sourceNode, "text", tkNode2, "text");
        subGraph.connect(tkNode2, "tokens2", hashTFNode, "tokenInput");
        subGraph.connect(hashTFNode, "TF", subGraph.sinkNode, "TF");

        SGraph graph = new SGraph("SubGraphOptimization");
        graph.addInputField("sentence");
        graph.addOutputField("token");
        graph.addOutputField("hashTF");

        Tokenizer tk1 = new Tokenizer();
        SNode tkNode1 = new SNode(tk1, "tokenizer1");
        tkNode1.addInputField("text");
        tkNode1.addOutputField("tokens1");

        graph.addNode(tkNode1);
        graph.addNode(subGraph);

        graph.connect(graph.sourceNode, "sentence", tkNode1, "text");
        graph.connect(tkNode1, "tokens1", graph.sinkNode, "token");

        graph.connect(graph.sourceNode, "sentence", subGraph, "text");
        graph.connect(subGraph, "TF", graph.sinkNode, "hashTF");

        graph.showGraph("subGraphOptimization_before_optimize");
        graph.optimize(graph);
        Dataset<Row> result = graph.toPipeline().fit(dataset).transform(dataset);
        graph.showGraph("subGraphOptimization_after_optimize");
        result.show();
    }

    @Test
    public void graphvizTest() throws IOException {
//        MutableGraph subGraph = mutGraph("subgraph");
//        MutableNode subSource = mutNode("source");
//        MutableNode subSink = mutNode("sink1");
//        subGraph.add(subSource.addLink(subSink));
//        subGraph.setCluster(true);
//
//        MutableGraph g = mutGraph("partentGraph");
//        MutableNode sourceNode = mutNode("sourceNode").addLink("tokenizer").addLink("source");
//        MutableNode tokenizer = mutNode("tokenizer").addLink("sink2");
//        g.add(subSink.addLink("sink2"));
//        g.add(sourceNode, tokenizer);
//        g.add(subGraph);

        MutableGraph sub = mutGraph("sub").add(
                node("source").link("sink1"),
                node("sink1").link("sink2"));
        sub.rootNodes().remove(node("sink1"));
        sub.rootNodes().remove(node("sink2"));
        MutableGraph g = mutGraph().add(
                node("sourceNode").link("tokenizer").link("source"),
                node("tokenizer").link("sink2")
        );
        g.add(sub.setCluster(true));
        Graphviz.fromGraph(g).render(Format.PNG).toFile(new File(String.format("figures/%s.png", "test")));

    }

}
