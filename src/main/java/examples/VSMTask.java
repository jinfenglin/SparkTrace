package examples;

import core.SparkTraceTask;
import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.SDF.SDFNode;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipeline.CosinSimilarityStage;
import featurePipeline.NullRemoveWrapper.NullRemoverModelSingleIO;
import featurePipeline.UnsupervisedStage.UnsupervisedStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.SparkSession;

/**
 * A trace task use VSM model
 */
public class VSMTask {

    public VSMTask() {

    }

    private static SDFGraph createSDF() throws Exception {
        SDFGraph sdf = new SDFGraph();
        sdf.setId("VSMTask_SDF");
        createSourceSDF(sdf);
        createTargetSDF(sdf);
        return sdf;
    }

    private static void createSourceSDF(SDFGraph sdfGraph) throws Exception {
        sdfGraph.addInputField("s_text");
        sdfGraph.addOutputField("s_tf_idf");
        SDFNode.SDFType type = SDFNode.SDFType.SOURCE_SDF;


        Tokenizer sTk = new Tokenizer();
        SDFNode tkNode = new SDFNode(new NullRemoverModelSingleIO(sTk), "source_tokenizer");
        tkNode.addInputField("s_text");
        tkNode.addOutputField("s_tokens");
        tkNode.assignTypeToOutputField("s_tokens", type);

        HashingTF htf = new HashingTF();
        SDFNode htfNode = new SDFNode(new NullRemoverModelSingleIO(htf), "source_hashingTF");
        htfNode.addInputField("s_tokens");
        htfNode.addOutputField("s_htf");
        htfNode.assignTypeToOutputField("s_htf", type);


        IDF idf = new IDF();
        UnsupervisedStage un_idf = new UnsupervisedStage(idf);
        SDFNode idfNode = new SDFNode(un_idf, "shared_IDF");
        idfNode.addInputField("s_idf_in");
        idfNode.addOutputField("s_idf_out");
        idfNode.assignTypeToOutputField("s_idf_out", type);

        sdfGraph.addNode(tkNode);
        sdfGraph.addNode(htfNode);
        sdfGraph.addNode(idfNode);

        sdfGraph.connectSymbol(sdfGraph.sourceNode, "s_text", tkNode, "s_text");
        sdfGraph.connectSymbol(tkNode, "s_tokens", htfNode, "s_tokens");
        sdfGraph.connectSymbol(htfNode, "s_htf", idfNode, "s_idf_in");
        sdfGraph.connectSymbol(idfNode, "s_idf_out", sdfGraph.sinkNode, "s_tf_idf");
    }

    private static void createTargetSDF(SDFGraph sdfGraph) throws Exception {
        sdfGraph.addInputField("t_text");
        sdfGraph.addOutputField("t_tf_idf");
        SDFNode.SDFType type = SDFNode.SDFType.TARGET_SDF;
        Tokenizer sTk = new Tokenizer();
        SDFNode tkNode = new SDFNode(new NullRemoverModelSingleIO(sTk), "target_tokenizer");
        tkNode.addInputField("t_text");
        tkNode.addOutputField("t_tokens");
        tkNode.assignTypeToOutputField("t_tokens", type);

        HashingTF htf = new HashingTF();
        SDFNode htfNode = new SDFNode(new NullRemoverModelSingleIO(htf), "target_hashingTF");
        htfNode.addInputField("t_tokens");
        htfNode.addOutputField("t_htf");
        htfNode.assignTypeToOutputField("t_htf", type);


        SDFNode idfNode = (SDFNode) sdfGraph.getNode("shared_IDF");
        idfNode.addInputField("t_idf_in");
        idfNode.addOutputField("t_idf_out");
        idfNode.assignTypeToOutputField("t_idf_out", type);

        sdfGraph.addNode(tkNode);
        sdfGraph.addNode(htfNode);
        sdfGraph.addNode(idfNode);

        sdfGraph.connectSymbol(sdfGraph.sourceNode, "t_text", tkNode, "t_text");
        sdfGraph.connectSymbol(tkNode, "t_tokens", htfNode, "t_tokens");
        sdfGraph.connectSymbol(htfNode, "t_htf", idfNode, "t_idf_in");
        sdfGraph.connectSymbol(idfNode, "t_idf_out", sdfGraph.sinkNode, "t_tf_idf");
    }

    private static SGraph createDDFGraph() throws Exception {
        SGraph ddfGraph = new SGraph("VSMTask_DDF");
        ddfGraph.addInputField("s_tf_idf");
        ddfGraph.addInputField("t_tf_idf");
        ddfGraph.addOutputField("vsm_cosin_sim_score");

        CosinSimilarityStage cosinSimilarityStage = new CosinSimilarityStage();
        SNode cosinNode = new SNode(cosinSimilarityStage);
        cosinNode.addInputField("vec1");
        cosinNode.addInputField("vec2");
        cosinNode.addOutputField("cosin_score");

        ddfGraph.addNode(cosinNode);

        ddfGraph.connectSymbol(ddfGraph.sourceNode, "s_tf_idf", cosinNode, "vec1");
        ddfGraph.connectSymbol(ddfGraph.sourceNode, "t_tf_idf", cosinNode, "vec2");
        ddfGraph.connectSymbol(cosinNode, "cosin_score", ddfGraph.sinkNode, "vsm_cosin_sim_score");
        return ddfGraph;
    }


    public static SparkTraceTask getSTT(SparkSession sparkSession) throws Exception {
        SparkTraceTask stt = new SparkTraceTask(sparkSession, createSDF(), createDDFGraph(), "s_id", "t_id");
        stt.setId("VSM_Task");
        stt.addInputField("s_id").addInputField("t_id").addInputField("s_text").addInputField("t_text");
        stt.addOutputField("vsm_score");

        stt.connectSymbol(stt.sourceNode, "s_id", stt.getSdfGraph(), "s_id");
        stt.connectSymbol(stt.sourceNode, "t_id", stt.getSdfGraph(), "t_id");
        stt.connectSymbol(stt.sourceNode, "s_text", stt.getSdfGraph(), "s_text");
        stt.connectSymbol(stt.sourceNode, "t_text", stt.getSdfGraph(), "t_text");

        stt.connectSymbol(stt.getSdfGraph(), "s_tf_idf", stt.getDdfGraph(), "s_tf_idf");
        stt.connectSymbol(stt.getSdfGraph(), "t_tf_idf", stt.getDdfGraph(), "t_tf_idf");

        stt.connectSymbol(stt.getDdfGraph(), "vsm_cosin_sim_score", stt.sinkNode, "vsm_score");
        return stt;
    }
}
