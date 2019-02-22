import buildingBlocks.preprocessor.EnglishPreprocess;
import buildingBlocks.preprocessor.NGramPreprocessPipeline;
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
import featurePipelineStages.cacheStage.CacheStage;
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

public class BuildingBlockTest extends TestBase {
    private static final String masterUrl = "local";
    Dataset<MavenCommit> commits;
    Dataset<MavenImprovement> improvements;
    Dataset<MavenLink> links;

    public BuildingBlockTest() {
        super(masterUrl);
    }

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
    public void EnglishPreprocessingPipelineTest() throws Exception {
        SGraph graph = EnglishPreprocess.getGraph("");
        Map config = new HashMap<>();
        config.put("text", "commit_content");
        graph.setConfig(config);
        graph.toPipeline().fit(commits).transform(commits).show(false);
    }

    @Test
    public void NgramPipeline() throws Exception {
        SGraph graph = NGramPreprocessPipeline.getGraph("ngram");
        Map config = new HashMap<>();
        config.put("text", "sentence");
        graph.setConfig(config);
        Dataset<Row> dataset = graph.toPipeline().fit(getSentenceLabelDataset()).transform(getSentenceLabelDataset());
        dataset.show();
    }

    @Test
    public void TFIDFPipelineTest() throws Exception {
        SGraph graph = Text2TFIDFPipeline.getGraph("tfidf");
        Map config = new HashMap<>();
        config.put("text1", "text1");
        config.put("text2", "text2");
        config.put("tf-idf1", "tf-idf1");
        config.put("tf-idf2", "tf-idf2");
        graph.setConfig(config);
        graph.toPipeline().fit(getMultiSentenceRowData()).transform(getMultiSentenceRowData());
    }

    @Test
    public void NgramTFIDF() throws Exception {
        SGraph graph = Text2NGramTFIDFPipeline.getGraph("tfidf");
        Map config = new HashMap<>();
        config.put("text1", "text1");
        config.put("text2", "text2");
        config.put("ngram-tf-idf1", "ngram-tf-idf1");
        config.put("ngram-tf-idf2", "ngram-tf-idf2");
        graph.setConfig(config);
        graph.toPipeline().fit(getMultiSentenceRowData()).transform(getMultiSentenceRowData()).show();
    }

    @Test
    public void LDAPipelineTest() throws Exception {
        SGraph graph = Text2LDAPipeline.getGraph("ldaTest");
        Map config = new HashMap<>();
        config.put("text1", "text1");
        config.put("text2", "text2");
        graph.setConfig(config);
        graph.toPipeline().fit(getMultiSentenceRowData()).transform(getMultiSentenceRowData()).show(false);
    }

    @Test
    public void ngramVSMTask() throws Exception {
        SparkTraceTask vsmTask = new NGramVSMTraceTask().getTask("s_id", "t_id");
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        vsmTask.setConfig(vsmTaskInputConfig);
        vsmTask.initSTT();
        vsmTask.infuse();
        vsmTask.optimize(vsmTask);
        vsmTask.train(commits, improvements, null);
        Dataset<Row> result = vsmTask.trace(commits, improvements);
        result.show();
    }

    @Test
    public void LDATask() throws Exception {
        SparkTraceTask ldaTask = new LDATraceBuilder().getTask("s_id", "t_id");
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        ldaTask.setConfig(vsmTaskInputConfig);
        ldaTask.initSTT();
        ldaTask.infuse();
        ldaTask.optimize(ldaTask);
        ldaTask.train(commits, improvements, null);
        Dataset<Row> result = ldaTask.trace(commits, improvements);
        result.show();
    }

    @Test
    public void voteTaskTest() throws Exception {
        SparkTraceTask voteTask = new VoteTraceBuilder().getTask("s_id", "t_id");
        Map<String, String> vsmTaskInputConfig = getVSMTaskConfig();
        voteTask.setConfig(vsmTaskInputConfig);
        voteTask.showGraph("votingSystem_before_optimize");
        voteTask.initSTT();
        voteTask.infuse();
        voteTask.optimize(voteTask);
        voteTask.showGraph("votingSystem_after_optimize");

        voteTask.train(commits, improvements, null);
        Dataset<Row> result = voteTask.trace(commits, improvements);
        result.show();
    }
}
