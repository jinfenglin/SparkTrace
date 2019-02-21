package examples;

import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.VecSimilarity.SparseVecSimilarity.SparseVecCosinSimilarityStage;
import featurePipelineStages.NullRemoveWrapper.NullRemoverModelSingleIO;
import featurePipelineStages.UnsupervisedStage.UnsupervisedStage;
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
        sdfGraph.addInputField("s_text").addInputField("s_id");
        sdfGraph.addOutputField("s_tf_idf", SDFGraph.SDFType.SOURCE_SDF);

        Tokenizer sTk = new Tokenizer();
        SNode tkNode = new SNode(new NullRemoverModelSingleIO(sTk), "source_tokenizer");
        tkNode.addInputField("s_text");
        tkNode.addOutputField("s_tokens");

        HashingTF htf = new HashingTF();
        SNode htfNode = new SNode(new NullRemoverModelSingleIO(htf), "source_hashingTF");
        htfNode.addInputField("s_tokens");
        htfNode.addOutputField("s_htf");


        IDF idf = new IDF();
        UnsupervisedStage un_idf = new UnsupervisedStage(idf);
        SNode idfNode = new SNode(un_idf, "shared_IDF");
        idfNode.addInputField("s_idf_in");
        idfNode.addOutputField("s_idf_out");

        sdfGraph.addNode(tkNode);
        sdfGraph.addNode(htfNode);
        sdfGraph.addNode(idfNode);

        sdfGraph.connect(sdfGraph.sourceNode, "s_text", tkNode, "s_text");
        sdfGraph.connect(tkNode, "s_tokens", htfNode, "s_tokens");
        sdfGraph.connect(htfNode, "s_htf", idfNode, "s_idf_in");
        sdfGraph.connect(idfNode, "s_idf_out", sdfGraph.sinkNode, "s_tf_idf");
    }

    private static void createTargetSDF(SDFGraph sdfGraph) throws Exception {
        sdfGraph.addInputField("t_id");
        sdfGraph.addInputField("t_text");
        sdfGraph.addOutputField("t_tf_idf", SDFGraph.SDFType.TARGET_SDF);

        Tokenizer sTk = new Tokenizer();
        SNode tkNode = new SNode(new NullRemoverModelSingleIO(sTk), "target_tokenizer");
        tkNode.addInputField("t_text");
        tkNode.addOutputField("t_tokens");

        HashingTF htf = new HashingTF();
        SNode htfNode = new SNode(new NullRemoverModelSingleIO(htf), "target_hashingTF");
        htfNode.addInputField("t_tokens");
        htfNode.addOutputField("t_htf");


        SNode idfNode = (SNode) sdfGraph.getNode("shared_IDF");
        idfNode.addInputField("t_idf_in");
        idfNode.addOutputField("t_idf_out");

        sdfGraph.addNode(tkNode);
        sdfGraph.addNode(htfNode);
        sdfGraph.addNode(idfNode);

        sdfGraph.connect(sdfGraph.sourceNode, "t_text", tkNode, "t_text");
        sdfGraph.connect(tkNode, "t_tokens", htfNode, "t_tokens");
        sdfGraph.connect(htfNode, "t_htf", idfNode, "t_idf_in");
        sdfGraph.connect(idfNode, "t_idf_out", sdfGraph.sinkNode, "t_tf_idf");
    }

    private static SGraph createDDFGraph() throws Exception {
        SGraph ddf = SparseCosinSimilarityPipeline.getGraph("VSM_DDF");//vec1,2 - cosin_sim
        return ddf;
    }


    public static SparkTraceTask getSTT(SparkSession sparkSession) throws Exception {
        SparkTraceTask stt = new SparkTraceTask(createSDF(), createDDFGraph(), "s_id", "t_id");
        stt.setId("VSM_Task");
        stt.addInputField("s_id").addInputField("t_id").addInputField("s_text").addInputField("t_text");
        stt.addOutputField("vsm_score");

        stt.connect(stt.sourceNode, "s_id", stt.getSdfGraph(), "s_id");
        stt.connect(stt.sourceNode, "t_id", stt.getSdfGraph(), "t_id");
        stt.connect(stt.sourceNode, "s_text", stt.getSdfGraph(), "s_text");
        stt.connect(stt.sourceNode, "t_text", stt.getSdfGraph(), "t_text");

        stt.connect(stt.getSdfGraph(), "s_tf_idf", stt.getDdfGraph(), "vec1");
        stt.connect(stt.getSdfGraph(), "t_tf_idf", stt.getDdfGraph(), "vec2");

        stt.connect(stt.getDdfGraph(), "cosin_sim", stt.sinkNode, "vsm_score");
        return stt;
    }
}
