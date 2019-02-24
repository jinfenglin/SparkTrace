import buildingBlocks.preprocessor.CleanTokens;
import buildingBlocks.preprocessor.NGramTokenizer;
import buildingBlocks.text2TFIDF.Text2LDAPipeline;
import buildingBlocks.text2TFIDF.Text2NGramTFIDFPipeline;
import buildingBlocks.text2TFIDF.Text2TFIDFPipeline;
import buildingBlocks.traceTasks.LDATraceBuilder;
import buildingBlocks.traceTasks.NGramVSMTraceTask;
import buildingBlocks.traceTasks.VSMTraceBuilder;
import buildingBlocks.traceTasks.VoteTraceBuilder;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;
import examples.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenLink;

import java.util.HashMap;
import java.util.Map;

import static core.graphPipeline.basic.SGraph.syncSymbolValues;


public class BuildingBlockTest extends TestBase {
    private static final String masterUrl = "local[4]";
    Dataset<MavenCommit> commits;
    Dataset<MavenImprovement> improvements;
    Dataset<MavenLink> links;

    public BuildingBlockTest() {
        super(masterUrl);
    }

    @Before
    public void runSparkTestWithMavenData() {
        String commitPath = "src/main/resources/maven_mini/commits.csv";
        String improvementPath = "src/main/resources/maven_mini/improvement.csv";
        String linkPath = "src/main/resources/maven_mini/improvementCommitLinks.csv";
        commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        links = TraceDatasetFactory.createDatasetFromCSV(sparkSession, linkPath, MavenLink.class);
    }

    @Test
    public void VSMTaskTest() throws Exception {
        SparkTraceTask vsmTask = new VSMTraceBuilder().getTask("s_id", "t_id");
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        vsmTask.setConfig(vsmTaskInputConfig);
        vsmTask.showGraph("singleSparkTaskTest_before");
        syncSymbolValues(vsmTask);
        long startTime = System.currentTimeMillis();
        vsmTask.train(commits, improvements, null);
        Dataset<Row> result = vsmTask.trace(commits, improvements);
        result.count();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.print(String.format("running time = %s\n", estimatedTime));
    }

    @Test
    public void ngramVSMTaskTest() throws Exception {
        SparkTraceTask vsmTask = new NGramVSMTraceTask().getTask("s_id", "t_id");
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        vsmTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(vsmTask);
        vsmTask.train(commits, improvements, null);
        Dataset<Row> result = vsmTask.trace(commits, improvements);
        result.count();
    }

    @Test
    public void LDATaskTest() throws Exception {
        SparkTraceTask ldaTask = new LDATraceBuilder().getTask("s_id", "t_id");
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        ldaTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(ldaTask);
        ldaTask.train(commits, improvements, null);
        Dataset<Row> result = ldaTask.trace(commits, improvements);
        result.count();
    }

    @Test
    public void voteTaskTest() throws Exception {
        SparkTraceTask voteTask = new VoteTraceBuilder().getTask("s_id", "t_id");
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        voteTask.setConfig(vsmTaskInputConfig);
        voteTask.showGraph("votingSystem_before_optimize");
        voteTask.initSTT();
        voteTask.optimize(voteTask);
        voteTask.showGraph("votingSystem_after_optimize");

        voteTask.train(commits, improvements, null);
        System.out.print("Training is finished...");
        Dataset<Row> result = voteTask.trace(commits, improvements);
        result.collectAsList();
    }
}
