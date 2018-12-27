package examples;

import core.SparkTraceTask;
import core.pipelineOptimizer.SDFGraph;
import core.pipelineOptimizer.SGraph;
import core.pipelineOptimizer.SNode;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;

/**
 *
 */
public class VSMTask {

    public VSMTask() {

    }

    private SDFGraph createSourceSDF() throws Exception {
        SDFGraph sourceSDF = new SDFGraph("source_id");
        sourceSDF.setId("VSMTask_SourceSDF");
        sourceSDF.setArtifactIdColName("s_id");
        sourceSDF.addInputField("s_id");
        sourceSDF.addInputField("s_text");
        sourceSDF.addOutputField("s_tf_idf");

        Tokenizer sTk = new Tokenizer();
        SNode tkNode = new SNode(sTk, "source_tokenizer");
        tkNode.addInputField("s_text");
        tkNode.addOutputField("s_tokens");

        HashingTF htf = new HashingTF();
        SNode htfNode = new SNode(htf, "source_hashingTF");
        htfNode.addInputField("s_tokens");
        htfNode.addOutputField("s_htf");

        IDF idf = new IDF();
        SNode idfNode = new SNode(idf, "source_IDF");
        idfNode.addInputField("s_idf_in");
        idfNode.addOutputField("s_idf_out");

        sourceSDF.addNode(tkNode);
        sourceSDF.addNode(htfNode);
        sourceSDF.addNode(idfNode);

        sourceSDF.connect(sourceSDF.sourceNode, "s_text", tkNode, "s_text");
        sourceSDF.connect(tkNode, "s_tokens", htfNode, "s_tokens");
        sourceSDF.connect(htfNode, "s_htf", idfNode, "s_idf_in");
        sourceSDF.connect(idfNode, "s_idf_out", sourceSDF.sinkNode, "s_tf_idf");
        return sourceSDF;
    }

    private SDFGraph createTargetSDF() throws Exception {
        SDFGraph targetSDF = new SDFGraph("target_id");
        targetSDF.setId("VSMTask_TargetSDF");
        targetSDF.setArtifactIdColName("t_id");
        targetSDF.addInputField("t_id");
        targetSDF.addInputField("t_text");
        targetSDF.addOutputField("t_tf_idf");

        Tokenizer sTk = new Tokenizer();
        SNode tkNode = new SNode(sTk, "target_tokenizer");
        tkNode.addInputField("t_text");
        tkNode.addOutputField("t_tokens");

        HashingTF htf = new HashingTF();
        SNode htfNode = new SNode(htf, "target_hashingTF");
        htfNode.addInputField("t_tokens");
        htfNode.addOutputField("t_htf");

        IDF idf = new IDF();
        SNode idfNode = new SNode(idf, "target_IDF");
        idfNode.addInputField("t_idf_in");
        idfNode.addOutputField("t_idf_out");

        targetSDF.addNode(tkNode);
        targetSDF.addNode(htfNode);
        targetSDF.addNode(idfNode);

        targetSDF.connect(targetSDF.sourceNode, "t_text", tkNode, "t_text");
        targetSDF.connect(tkNode, "t_tokens", htfNode, "t_tokens");
        targetSDF.connect(htfNode, "t_htf", idfNode, "t_idf_in");
        targetSDF.connect(idfNode, "t_idf_out", targetSDF.sinkNode, "t_tf_idf");

        return targetSDF;
    }

    private SGraph createDDFGraph() {
        SGraph ddfGraph = new SGraph("VSM_DDFGraph");
        

        return null;
    }

    private SGraph createTraceModel() {
        return null;
    }

    public static SparkTraceTask getSTT() throws Exception {


        return null;
    }
}
