import traceTasks.LDATraceBuilder;
import traceTasks.NGramVSMTraceTaskBuilder;
import traceTasks.VSMTraceBuilder;
import traceTasks.OptimizedVoteTraceBuilder;
import core.SparkTraceTask;
import examples.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import scala.collection.Seq;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenICLink;

import java.util.Arrays;
import java.util.Map;

import static core.graphPipeline.SLayer.SGraph.syncSymbolValues;

/**
 * BuildingBlocks test and voting system test on Maven commit-improvement
 */
public class BuildingBlockTest extends TestBase {
    private static final String masterUrl = "local[4]";
    Dataset<MavenCommit> commits;
    Dataset<MavenImprovement> improvements;
    Dataset<MavenICLink> links;

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
        links = TraceDatasetFactory.createDatasetFromCSV(sparkSession, linkPath, MavenICLink.class);
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
        SparkTraceTask vsmTask = new NGramVSMTraceTaskBuilder().getTask("s_id", "t_id");
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
        String sourceId = "s_id", targetId = "t_id";
        SparkTraceTask voteTask = new OptimizedVoteTraceBuilder().getTask(sourceId, targetId);
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        voteTask.setConfig(vsmTaskInputConfig);
        voteTask.showGraph("votingSystem_before_optimize_new");
        voteTask.getSourceSDFSdfGraph().optimize(voteTask.getSourceSDFSdfGraph());
        voteTask.getTargetSDFSdfGraph().optimize(voteTask.getTargetSDFSdfGraph());
        voteTask.showGraph("votingSystem_after_optimize_new");
        syncSymbolValues(voteTask);
        voteTask.train(commits, improvements, null);
        System.out.print("Training is finished...");
        Dataset<Row> result = voteTask.trace(commits, improvements);

        String vsmScoreCol = voteTask.getOutputField(OptimizedVoteTraceBuilder.VSM_SCORE).getFieldSymbol().getSymbolValue();
        String ngramVsmScoreCol = voteTask.getOutputField(OptimizedVoteTraceBuilder.NGRAM_SCORE).getFieldSymbol().getSymbolValue();
        String ldaScoreCol = voteTask.getOutputField(OptimizedVoteTraceBuilder.LDA_SCORE).getFieldSymbol().getSymbolValue();

        result.select(vsmTaskInputConfig.get(sourceId), vsmTaskInputConfig.get(targetId), vsmScoreCol, ngramVsmScoreCol, ldaScoreCol).write()
                .format("com.databricks.spark.csv")
                .option("header", "true").mode("overwrite")
                .save("results/voteResult.csv");
    }

    @Test
    public void voteTaskTestNotOptimized() throws Exception {
        String sourceId = "s_id", targetId = "t_id";

        SparkTraceTask vsmTask = new VSMTraceBuilder().getTask(sourceId, targetId);
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        String s_id_col_name = vsmTaskInputConfig.get(sourceId);
        String t_id_col_name = vsmTaskInputConfig.get(targetId);

        vsmTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(vsmTask);

        vsmTask.train(commits, improvements, null);
        Dataset<Row> result1 = vsmTask.trace(commits, improvements);

        SparkTraceTask ngramTask = new NGramVSMTraceTaskBuilder().getTask(sourceId, targetId);
        ngramTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(ngramTask);
        ngramTask.train(commits, improvements, null);
        Dataset<Row> result2 = ngramTask.trace(commits, improvements);

        SparkTraceTask ldaTask = new LDATraceBuilder().getTask(sourceId, targetId);
        ldaTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(ldaTask);
        ldaTask.train(commits, improvements, null);
        Dataset<Row> result3 = ldaTask.trace(commits, improvements);

        String vsmScoreCol = vsmTask.getOutputField(VSMTraceBuilder.OUTPUT).getFieldSymbol().getSymbolValue();
        String ngramVsmScoreCol = ngramTask.getOutputField(NGramVSMTraceTaskBuilder.OUTPUT).getFieldSymbol().getSymbolValue();
        String ldaScoreCol = ldaTask.getOutputField(LDATraceBuilder.OUTPUT).getFieldSymbol().getSymbolValue();

        Seq<String> colNames = scala.collection.JavaConverters.asScalaIteratorConverter(
                Arrays.asList(s_id_col_name, t_id_col_name).iterator()
        ).asScala().toSeq();
        Dataset<Row> result = result1.join(result2, colNames);
        result = result.join(result3, colNames);

        result.select(vsmTaskInputConfig.get(sourceId), vsmTaskInputConfig.get(targetId), vsmScoreCol, ngramVsmScoreCol, ldaScoreCol).write()
                .format("com.databricks.spark.csv")
                .option("header", "true").mode("overwrite")
                .save("results/voteResult.csv");
    }
}
