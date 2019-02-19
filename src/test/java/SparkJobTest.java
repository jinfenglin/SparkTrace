import core.SparkTraceTask;
import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.SDF.SDFNode;
import core.graphPipeline.basic.SGraph;
import examples.TestBase;
import examples.VSMTask;
import featurePipeline.NullRemoveWrapper.NullRemoverModelSingleIO;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenLink;

import java.util.*;

public class SparkJobTest extends TestBase {
    private static String masterUrl = "local";

    public SparkJobTest() {
        super(masterUrl);
    }

    Dataset<MavenCommit> commits;
    Dataset<MavenImprovement> improvements;
    Dataset<MavenLink> links;

    @Before
    public void runSparkTestWithMavenData() {
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        String linkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        links = TraceDatasetFactory.createDatasetFromCSV(sparkSession, linkPath, MavenLink.class);
    }

    @Test
    public void multiDuplicatedStagePerformanceTest() {
        Dataset<Row> dataset = getSentenceLabelDataset();
        Pipeline pipelineMulti = new Pipeline();
        List<PipelineStage> stageList = new ArrayList<>();
        long endTime = 0;
        long startTime = 0;

        for (int i = 0; i < 200; i += 1) {
            Tokenizer tmp_tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("token_" + i);
            stageList.add(tmp_tokenizer);
        }
        startTime = System.currentTimeMillis();
        pipelineMulti.setStages(stageList.toArray(new PipelineStage[0]));
        pipelineMulti.fit(dataset).transform(dataset);
        endTime = System.currentTimeMillis();
        long multiStageTime = endTime - startTime;
        System.out.println("multi stage time:" + multiStageTime);
    }

    @Test
    public void singleSparkTaskTest() throws Exception {
        SparkTraceTask vsmTask = VSMTask.getSTT(sparkSession);
        vsmTask.initSTT();
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        vsmTask.setConfig(vsmTaskInputConfig);
        vsmTask.showGraph("singleSparkTaskTest");
        vsmTask.train(commits, improvements, null);
        Dataset<Row> result = vsmTask.trace(commits, improvements);
        result.show();
        System.out.print(String.format("Total link num: %s are processed...", result.count()));
    }

    @Test
    public void nestedSparkTaskTest() throws Exception {

        //create SDF
        SDFGraph sdfGraph = new SDFGraph();
        sdfGraph.setId("nestedTask_SDF");
        sdfGraph.addInputField("s_t");
        sdfGraph.addInputField("t_t");
        sdfGraph.addOutputField("s_tk");
        sdfGraph.addOutputField("t_tk");

        Map<String, String> nestTaskConfig = new HashMap<>();
        nestTaskConfig.put("s_t", "commit_content");
        nestTaskConfig.put("t_t", "issue_content");
        nestTaskConfig.put("s_id", "commit_id");
        nestTaskConfig.put("t_id", "issue_id");

        Tokenizer sTk = new Tokenizer();
        SDFNode stkNode = new SDFNode(new NullRemoverModelSingleIO(sTk), "source_tokenizer");
        stkNode.addInputField("s_text");
        stkNode.addOutputField("s_tokens");
        stkNode.assignTypeToOutputField("s_tokens", SDFNode.SDFType.SOURCE_SDF);

        Tokenizer tTk = new Tokenizer();
        SDFNode ttkNode = new SDFNode(new NullRemoverModelSingleIO(tTk), "target_tokenizer");
        ttkNode.addInputField("t_text");
        ttkNode.addOutputField("t_tokens");
        ttkNode.assignTypeToOutputField("t_tokens", SDFNode.SDFType.TARGET_SDF);

        sdfGraph.addNode(stkNode);
        sdfGraph.addNode(ttkNode);
        sdfGraph.connect(sdfGraph.sourceNode, "s_t", stkNode, "s_text");
        sdfGraph.connect(sdfGraph.sourceNode, "t_t", ttkNode, "t_text");
        sdfGraph.connect(stkNode, "s_tokens", sdfGraph.sinkNode, "s_tk");
        sdfGraph.connect(ttkNode, "t_tokens", sdfGraph.sinkNode, "t_tk");


        //create DDF
        SGraph ddfGraph = new SGraph("nestedTask_DDF");
        ddfGraph.addInputField("s_tf_idf");
        ddfGraph.addInputField("t_tf_idf");
        ddfGraph.addOutputField("tk1");
        ddfGraph.addOutputField("tk2");
        ddfGraph.addOutputField("similarity");

//        SparkTraceTask task = VSMTask.getSTT(sparkSession);
//        task.getSdfGraph().configSDF(getVSMTaskConfig());
//        ddfGraph.addNode(task);
        ddfGraph.connect(ddfGraph.sourceNode, "s_tf_idf", ddfGraph.sinkNode, "tk1");
        ddfGraph.connect(ddfGraph.sourceNode, "t_tf_idf", ddfGraph.sinkNode, "tk2");
        //ddfGraph.connectSymbol(task, "vsm_cosin_sim_score", ddfGraph, "similarity");

        SparkTraceTask outerTask = new SparkTraceTask(sdfGraph, ddfGraph, "s_id", "t_id");
        outerTask.connect(sdfGraph, "s_tk", ddfGraph, "s_tf_idf");
        outerTask.connect(sdfGraph, "t_tk", ddfGraph, "t_tf_idf");
        outerTask.initSTT();
        //Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        //outerTask.getSdfGraph().configSDF(vsmTaskInputConfig);
        outerTask.train(commits, improvements, null);
        Dataset<Row> result = outerTask.trace(commits, improvements);
        result.show();
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

        SparkTraceTask context = new SparkTraceTask(sdf, ddf, "s_id", "t_id");
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
        context.infuse();
        context.optimize(context);
        context.showGraph("TaskMergeTest_after_optimize");
        context.setConfig(getVSMTaskConfig());

        context.train(commits, improvements, null);
        Dataset<Row> result = context.trace(commits, improvements);
        result.show();
    }
}
