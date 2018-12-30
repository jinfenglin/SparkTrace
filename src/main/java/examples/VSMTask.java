package examples;

import core.SparkTraceTask;
import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.SDF.SDFNode;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipeline.CosinSimilarityStage;
import featurePipeline.UnsupervisedStage.UnsupervisedStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;

/**
 * A trace task use VSM model
 */
public class VSMTask {

    public VSMTask() {

    }

    private static SDFGraph createSDF() throws Exception {
        SDFGraph sdf = new SDFGraph("s_id", "t_id");
        sdf.setId("VSMTask_SDF");
        createSourceSDF(sdf);
        createTargetSDF(sdf);
        return sdf;
    }

    private static void createSourceSDF(SDFGraph sdfGraph) throws Exception {
        sdfGraph.addInputField("s_text");
        sdfGraph.addOutputField("s_tf_idf");
        SDFNode.SDFType type = SDFNode.SDFType.SOURCE_ART_SDF;


        Tokenizer sTk = new Tokenizer();
        SDFNode tkNode = new SDFNode(sTk, "source_tokenizer", type);
        tkNode.addInputField("s_text");
        tkNode.addOutputField("s_tokens");

        HashingTF htf = new HashingTF();
        SDFNode htfNode = new SDFNode(htf, "source_hashingTF", type);
        htfNode.addInputField("s_tokens");
        htfNode.addOutputField("s_htf");


        IDF idf = new IDF();
        UnsupervisedStage un_idf = new UnsupervisedStage(idf);
        SDFNode idfNode = new SDFNode(un_idf, "shared_IDF", SDFNode.SDFType.SHARED_SDF);
        idfNode.addInputField("s_idf_in");
        idfNode.addOutputField("s_idf_out", type);

        sdfGraph.addNode(tkNode);
        sdfGraph.addNode(htfNode);
        sdfGraph.addNode(idfNode);

        sdfGraph.connect(sdfGraph.sourceNode, "s_text", tkNode, "s_text");
        sdfGraph.connect(tkNode, "s_tokens", htfNode, "s_tokens");
        sdfGraph.connect(htfNode, "s_htf", idfNode, "s_idf_in");
        sdfGraph.connect(idfNode, "s_idf_out", sdfGraph.sinkNode, "s_tf_idf");
    }

    private static void createTargetSDF(SDFGraph sdfGraph) throws Exception {
        sdfGraph.addInputField("t_text");
        sdfGraph.addOutputField("t_tf_idf");
        SDFNode.SDFType type = SDFNode.SDFType.TARGET_ART_SDF;
        Tokenizer sTk = new Tokenizer();
        SDFNode tkNode = new SDFNode(sTk, "target_tokenizer", type);
        tkNode.addInputField("t_text");
        tkNode.addOutputField("t_tokens");

        HashingTF htf = new HashingTF();
        SDFNode htfNode = new SDFNode(htf, "target_hashingTF", type);
        htfNode.addInputField("t_tokens");
        htfNode.addOutputField("t_htf");


        SDFNode idfNode = (SDFNode) sdfGraph.getNode("shared_IDF");
        idfNode.addInputField("t_idf_in");
        idfNode.addOutputField("t_idf_out", type);

        sdfGraph.addNode(tkNode);
        sdfGraph.addNode(htfNode);
        sdfGraph.addNode(idfNode);

        sdfGraph.connect(sdfGraph.sourceNode, "t_text", tkNode, "t_text");
        sdfGraph.connect(tkNode, "t_tokens", htfNode, "t_tokens");
        sdfGraph.connect(htfNode, "t_htf", idfNode, "t_idf_in");
        sdfGraph.connect(idfNode, "t_idf_out", sdfGraph.sinkNode, "t_tf_idf");
    }

    private static SGraph createDDFGraph() throws Exception {
        SGraph ddfGraph = new SGraph("VSM_DDFGraph");
        ddfGraph.addInputField("s_tf_idf");
        ddfGraph.addInputField("t_tf_idf");
        ddfGraph.addOutputField("vsm_cosin_sim_score");

        CosinSimilarityStage cosinSimilarityStage = new CosinSimilarityStage();
        SNode cosinNode = new SNode(cosinSimilarityStage);
        cosinNode.addInputField("vec1");
        cosinNode.addInputField("vec2");
        cosinNode.addOutputField("cosin_score");

        ddfGraph.addNode(cosinNode);

        ddfGraph.connect(ddfGraph.sourceNode, "s_tf_idf", cosinNode, "vec1");
        ddfGraph.connect(ddfGraph.sourceNode, "t_tf_idf", cosinNode, "vec2");
        ddfGraph.connect(cosinNode, "cosin_score", ddfGraph.sinkNode, "vsm_cosin_sim_score");
        return ddfGraph;
    }


    public static SparkTraceTask getSTT() throws Exception {
        SparkTraceTask stt = new SparkTraceTask();
        stt.setId("VSM_Task");
        stt.setSdfGraph(createSDF());
        stt.setDdfGraph(createDDFGraph());
        stt.connect(stt.getSdfGraph(), "s_tf_idf", stt.getDdfGraph(), "s_tf_idf");
        stt.connect(stt.getSdfGraph(), "t_tf_idf", stt.getDdfGraph(), "t_tf_idf");
        return stt;
    }
}
