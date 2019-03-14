import examples.TestBase;
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
import traceability.components.maven.MavenICLink;

import java.util.*;

public class SparkJobTest extends TestBase {
    private static String masterUrl = "local[1]";

    public SparkJobTest() {
        super(masterUrl);
    }

    Dataset<MavenCommit> commits;
    Dataset<MavenImprovement> improvements;
    Dataset<MavenICLink> links;

    @Before
    public void runSparkTestWithMavenData() {
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        String linkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        links = TraceDatasetFactory.createDatasetFromCSV(sparkSession, linkPath, MavenICLink.class);
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
    public void TaskMergeTest() throws Exception {
//        SparkTraceTask t1 = new VSMTraceBuilder().getTask("s_id","t_id");
//        SGraph sdf = new SGraph();
//        sdf.setVertexLabel("ParentTask_SDF");
//        sdf.addInputField("s_id").addInputField("t_id").addInputField("s_text").addInputField("t_text");
//        sdf.addOutputField("s_text_out", SGraph.SDFType.SOURCE_SDF);
//        sdf.addOutputField("t_text_out", SGraph.SDFType.TARGET_SDF);
//        sdf.addOutputField("s_id_out", SGraph.SDFType.SOURCE_SDF);
//        sdf.addOutputField("t_id_out", SGraph.SDFType.TARGET_SDF);
//
//        sdf.connect(sdf.sourceNode, "s_text", sdf.sinkNode, "s_text_out");
//        sdf.connect(sdf.sourceNode, "t_text", sdf.sinkNode, "t_text_out");
//        sdf.connect(sdf.sourceNode, "t_id", sdf.sinkNode, "t_id_out");
//        sdf.connect(sdf.sourceNode, "s_id", sdf.sinkNode, "s_id_out");
//
//        SGraph ddf = new SGraph();
//        ddf.addInputField("s_text").addInputField("s_id");
//        ddf.addInputField("t_text").addInputField("t_id");
//        ddf.addOutputField("vsm_cosin_sim_score");
//        ddf.setVertexLabel("ParentTask_DDF");
//
//        ddf.addNode(t1);
//        ddf.connect(ddf.sourceNode, "s_text", t1, "s_text");
//        ddf.connect(ddf.sourceNode, "t_text", t1, "t_text");
//        ddf.connect(ddf.sourceNode, "s_id", t1, "s_id");
//        ddf.connect(ddf.sourceNode, "t_id", t1, "t_id");
//        ddf.connect(t1, "vsm_score", ddf.sinkNode, "vsm_cosin_sim_score");
//
//        SparkTraceTask context = new SparkTraceTask(sdf, ddf, "s_id", "t_id");
//        context.setVertexLabel("ContextTask");
//        context.addInputField("s_id").addInputField("t_id").addInputField("s_text").addInputField("t_text");
//        context.addOutputField("vsm_score");
//        context.connect(context.sourceNode, "s_id", sdf, "s_id");
//        context.connect(context.sourceNode, "t_id", sdf, "t_id");
//        context.connect(context.sourceNode, "s_text", sdf, "s_text");
//        context.connect(context.sourceNode, "t_text", sdf, "t_text");
//
//        context.connect(context.getSdfGraph(), "s_text_out", context.getDdfGraph(), "s_text");
//        context.connect(context.getSdfGraph(), "t_text_out", context.getDdfGraph(), "t_text");
//        context.connect(context.getSdfGraph(), "s_id_out", context.getDdfGraph(), "s_id");
//        context.connect(context.getSdfGraph(), "t_id_out", context.getDdfGraph(), "t_id");
//        context.connect(context.getDdfGraph(), "vsm_cosin_sim_score", context.sinkNode, "vsm_score");
//
//        context.showGraph("TaskMergeTest_before_optimize");
//        context.initSTT();
//        context.infuse();
//        context.optimize(context);
//        context.showGraph("TaskMergeTest_after_optimize");
//        context.setConfig(getVSMTaskConfig());
//
//        context.train(commits, improvements, null);
//        Dataset<Row> result = context.trace(commits, improvements);
//        result.show();
    }
}
