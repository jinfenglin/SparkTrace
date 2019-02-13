import core.SparkTraceTask;
import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.SDF.SDFNode;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import examples.TestBase;
import examples.VSMTask;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

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
        graph.showGraph("subGraphOptimization_after_optimize");
        Dataset<Row> result = graph.toPipeline().fit(dataset).transform(dataset);
        result.show();
    }

    @Test
    public void dualSubGraphTest() throws Exception {
        Dataset<Row> dataset = getSentenceLabelDataset();

        SGraph subGraph1 = new SGraph("VSMGraph");
        subGraph1.addInputField("text");
        subGraph1.addOutputField("TF-IDF");

        Tokenizer tk1 = new Tokenizer();
        SNode tkNode1 = new SNode(tk1, "tokenizer1");
        tkNode1.addInputField("text");
        tkNode1.addOutputField("tokens1");

        HashingTF hashingTF1 = new HashingTF();
        SNode hashTFNode1 = new SNode(hashingTF1, "hashTF1");
        hashTFNode1.addInputField("tokenInput");
        hashTFNode1.addOutputField("TF");

        IDF idf1 = new IDF();
        SNode idfNode = new SDFNode(idf1, "idf1");
        idfNode.addInputField("s_idf_in");
        idfNode.addOutputField("s_idf_out");

        subGraph1.addNode(tkNode1);
        subGraph1.addNode(hashTFNode1);
        subGraph1.addNode(idfNode);

        subGraph1.connect(subGraph1.sourceNode, "text", tkNode1, "text");
        subGraph1.connect(tkNode1, "tokens1", hashTFNode1, "tokenInput");
        subGraph1.connect(hashTFNode1, "TF", idfNode, "s_idf_in");
        subGraph1.connect(idfNode, "s_idf_out", subGraph1.sinkNode, "TF-IDF");


        SGraph subGraph2 = new SGraph("HashTFGraph");
        subGraph2.addInputField("text");
        subGraph2.addOutputField("TF");

        Tokenizer tk2 = new Tokenizer();
        SNode tkNode2 = new SNode(tk2, "tokenizer2");
        tkNode2.addInputField("text");
        tkNode2.addOutputField("tokens2");

        HashingTF hashingTF2 = new HashingTF();
        SNode hashTFNode2 = new SNode(hashingTF2, "hashTF2");
        hashTFNode2.addInputField("tokenInput");
        hashTFNode2.addOutputField("TF");

        subGraph2.addNode(tkNode2);
        subGraph2.addNode(hashTFNode2);

        subGraph2.connect(subGraph2.sourceNode, "text", tkNode2, "text");
        subGraph2.connect(tkNode2, "tokens2", hashTFNode2, "tokenInput");
        subGraph2.connect(hashTFNode2, "TF", subGraph2.sinkNode, "TF");

        SGraph graph = new SGraph("dualSubGraphTest");
        graph.addInputField("sentence");
        graph.addOutputField("TF");
        graph.addOutputField("TF-IDF");

        graph.addNode(subGraph1);
        graph.addNode(subGraph2);

        graph.connect(graph.sourceNode, "sentence", subGraph1, "text");
        graph.connect(graph.sourceNode, "sentence", subGraph2, "text");
        graph.connect(subGraph1, "TF-IDF", graph.sinkNode, "TF-IDF");
        graph.connect(subGraph2, "TF", graph.sinkNode, "TF");

        graph.showGraph("dual_subGraph_before_optimize");
        graph.optimize(graph);
        graph.showGraph("dual_subGraph__after_optimize");
        Dataset<Row> result = graph.toPipeline().fit(dataset).transform(dataset);
        result.show();
    }

    @Test
    public void subSubGraphTest() throws Exception {
        Dataset<Row> dataset = getSentenceLabelDataset();
        //Create VSM graph contain subgraph
        SGraph VSMGraph = new SGraph("VSMGraph");
        VSMGraph.addInputField("text");
        VSMGraph.addOutputField("TF-IDF");
        SGraph htfSubGraph = createHTFSubGraph("HTFSubGraph");
        IDF idf = new IDF();
        SNode idfNode = new SDFNode(idf, "idf1");
        idfNode.addInputField("s_idf_in");
        idfNode.addOutputField("s_idf_out");

        VSMGraph.addNode(htfSubGraph);
        VSMGraph.addNode(idfNode);

        VSMGraph.connect(VSMGraph.sourceNode, "text", htfSubGraph, "text");
        VSMGraph.connect(htfSubGraph, "TF", idfNode, "s_idf_in");
        VSMGraph.connect(idfNode, "s_idf_out", VSMGraph.sinkNode, "TF-IDF");

        //Create HF graph contain no sub graph
        SGraph htfGraph = createHTFSubGraph("HTF");

        //Create back ground graph
        SGraph graph = new SGraph("subSubGraphTest");
        graph.addInputField("sentence");
        graph.addOutputField("TF");
        graph.addOutputField("TF-IDF");

        graph.addNode(VSMGraph);
        graph.addNode(htfGraph);

        graph.connect(graph.sourceNode, "sentence", VSMGraph, "text");
        graph.connect(graph.sourceNode, "sentence", htfGraph, "text");
        graph.connect(VSMGraph, "TF-IDF", graph.sinkNode, "TF-IDF");
        graph.connect(htfGraph, "TF", graph.sinkNode, "TF");

        graph.showGraph("subSubGraphTest_before_optimize");
        graph.optimize(graph);
        graph.showGraph("subSubGraphTest_after_optimize");
        Dataset<Row> result = graph.toPipeline().fit(dataset).transform(dataset);
        result.show();

    }

    @Test
    public void dualSubSubGraphTest() throws Exception {
        Dataset<Row> dataset = getSentenceLabelDataset();
        //Create VSM graph 1 contain subgraph
        SGraph VSMGraph1 = new SGraph("VSMGraph1");
        VSMGraph1.addInputField("text");
        VSMGraph1.addOutputField("TF-IDF");
        SGraph htfSubGraph = createHTFSubGraph("HTFSubGraph");
        IDF idf = new IDF();
        SNode idfNode = new SDFNode(idf, "idf1");
        idfNode.addInputField("s_idf_in");
        idfNode.addOutputField("s_idf_out");

        VSMGraph1.addNode(htfSubGraph);
        VSMGraph1.addNode(idfNode);

        VSMGraph1.connect(VSMGraph1.sourceNode, "text", htfSubGraph, "text");
        VSMGraph1.connect(htfSubGraph, "TF", idfNode, "s_idf_in");
        VSMGraph1.connect(idfNode, "s_idf_out", VSMGraph1.sinkNode, "TF-IDF");

        //Create VSM graph 2 contain no subgraph
        String graphId = "VSMGraph2";
        SGraph VSMGraph2 = new SGraph(graphId);
        VSMGraph2.addInputField("text");
        VSMGraph2.addOutputField("TF-IDF");

        Tokenizer tk2 = new Tokenizer();
        SNode tkNode2 = new SNode(tk2, graphId + "_tokenizer");
        tkNode2.addInputField("text");
        tkNode2.addOutputField("tokens");

        HashingTF hashingTF2 = new HashingTF();
        SNode hashTFNode2 = new SNode(hashingTF2, graphId + "_hashTF");
        hashTFNode2.addInputField("tokenInput");
        hashTFNode2.addOutputField("TF");

        IDF idf2 = new IDF();
        SNode idfNode2 = new SDFNode(idf2, "idf2");
        idfNode2.addInputField("s_idf_in");
        idfNode2.addOutputField("s_idf_out");

        VSMGraph2.addNode(tkNode2);
        VSMGraph2.addNode(hashTFNode2);
        VSMGraph2.addNode(idfNode2);


        VSMGraph2.connect(VSMGraph2.sourceNode, "text", tkNode2, "text");
        VSMGraph2.connect(tkNode2, "tokens", hashTFNode2, "tokenInput");
        VSMGraph2.connect(hashTFNode2, "TF", idfNode2, "s_idf_in");
        VSMGraph2.connect(idfNode2, "s_idf_out", VSMGraph2.sinkNode, "TF-IDF");

        // Add the two VSM to background graph
        SGraph graph = new SGraph("dualSubSubGraphTest");
        graph.addInputField("sentence");
        graph.addOutputField("TF-IDF1");
        graph.addOutputField("TF-IDF2");

        graph.addNode(VSMGraph1);
        graph.addNode(VSMGraph2);

        graph.connect(graph.sourceNode, "sentence", VSMGraph1, "text");
        graph.connect(graph.sourceNode, "sentence", VSMGraph2, "text");
        graph.connect(VSMGraph1, "TF-IDF", graph.sinkNode, "TF-IDF1");
        graph.connect(VSMGraph2, "TF-IDF", graph.sinkNode, "TF-IDF2");

        graph.showGraph("subSubGraphTest_before_optimize_before_optimize");
        graph.optimize(graph);
        graph.showGraph("subSubGraphTest_before_optimize_after_optimize");
        Dataset<Row> result = graph.toPipeline().fit(dataset).transform(dataset);
        result.show();

    }

    /**
     * Create a sub graph with tokenizer and hashtf
     *
     * @return
     */
    private SGraph createHTFSubGraph(String graphId) throws Exception {
        SGraph subGraph = new SGraph(graphId);
        subGraph.addInputField("text");
        subGraph.addOutputField("TF");

        Tokenizer tk2 = new Tokenizer();
        SNode tkNode2 = new SNode(tk2, graphId + "_tokenizer");
        tkNode2.addInputField("text");
        tkNode2.addOutputField("tokens");

        HashingTF hashingTF2 = new HashingTF();
        SNode hashTFNode2 = new SNode(hashingTF2, graphId + "_hashTF");
        hashTFNode2.addInputField("tokenInput");
        hashTFNode2.addOutputField("TF");

        subGraph.addNode(tkNode2);
        subGraph.addNode(hashTFNode2);

        subGraph.connect(subGraph.sourceNode, "text", tkNode2, "text");
        subGraph.connect(tkNode2, "tokens", hashTFNode2, "tokenInput");
        subGraph.connect(hashTFNode2, "TF", subGraph.sinkNode, "TF");

        return subGraph;
    }

    @Test
    public void TaskMergeTest() throws Exception {
        SparkTraceTask t1 = VSMTask.getSTT(sparkSession);
        SDFGraph sdf = new SDFGraph();
        sdf.setId("ParentTask_SDF");
        sdf.addInputField("s_id").addInputField("t_id").addInputField("s_text").addInputField("t_text");
        sdf.addOutputField("s_text_out").addOutputField("t_text_out").addOutputField("s_id_out").addOutputField("t_id_out");
        sdf.assignTypeToOutputField("s_text_out", SDFNode.SDFType.SOURCE_SDF);
        sdf.assignTypeToOutputField("t_text_out", SDFNode.SDFType.TARGET_SDF);
        sdf.connect(sdf.sourceNode, "s_text", sdf.sinkNode, "s_text_out");
        sdf.connect(sdf.sourceNode, "t_text", sdf.sinkNode, "t_text_out");
        sdf.connect(sdf.sourceNode, "t_id", sdf.sinkNode, "t_id_out");
        sdf.connect(sdf.sourceNode, "s_id", sdf.sinkNode, "s_id_out");

        SGraph ddf = new SGraph();
        ddf.addInputField("s_text").addInputField("s_id");
        ddf.addInputField("t_text").addInputField("t_id");
        ddf.addOutputField("vsm_cosin_sim_score");
        ddf.setId("ParentTask_DDF");

        ddf.addNode(t1);
        ddf.connect(ddf.sourceNode, "s_text", t1, "s_text");
        ddf.connect(ddf.sourceNode, "t_text", t1, "t_text");
        ddf.connect(ddf.sourceNode, "s_id", t1, "s_id");
        ddf.connect(ddf.sourceNode, "t_id", t1, "t_id");
        ddf.connect(t1, "vsm_score", ddf.sinkNode, "vsm_cosin_sim_score");

        SparkTraceTask context = new SparkTraceTask(sparkSession, sdf, ddf, "s_id", "t_id");
        context.setId("ContextTask");
        context.addInputField("s_id").addInputField("t_id").addInputField("s_text").addInputField("t_text");
        context.addOutputField("vsm_score");
        context.connect(context.sourceNode, "s_id", sdf, "s_id");
        context.connect(context.sourceNode, "t_id", sdf, "t_id");
        context.connect(context.sourceNode, "s_text", sdf, "s_text");
        context.connect(context.sourceNode, "t_text", sdf, "t_text");

        context.connect(context.getSdfGraph(), "s_text_out", context.getDdfGraph(), "s_text");
        context.connect(context.getSdfGraph(), "t_text_out", context.getDdfGraph(), "t_text");
        context.connect(context.getSdfGraph(), "s_id_out", context.getDdfGraph(), "s_id");
        context.connect(context.getSdfGraph(), "t_id_out", context.getDdfGraph(), "t_id");
        context.connect(context.getDdfGraph(), "vsm_cosin_sim_score", context.sinkNode, "vsm_score");

        context.showGraph("TaskMergeTest_before_optimize");
        context.initSTT();
        context.showGraph("TaskMergeTest_after_optimize");
    }

}
