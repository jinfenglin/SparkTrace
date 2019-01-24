import core.SparkTraceTask;
import examples.TestBase;
import examples.VSMTask;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        SparkTraceTask vsmTask = VSMTask.getSTT();
        vsmTask.initSTT();
        Map<String, String> vsmTaskInputConfig = new HashMap<>();
        vsmTaskInputConfig.put("s_text", "commit_content");
        vsmTaskInputConfig.put("t_text", "issue_content");
        vsmTaskInputConfig.put("s_id", "commit_id");
        vsmTaskInputConfig.put("t_id", "issue_id");
        vsmTask.getSdfGraph().configSDF(vsmTaskInputConfig);
        vsmTask.train(commits, improvements, null);
        vsmTask.trace(commits, improvements);
    }

    @Test
    public void nestedSparkTaskTest() {
    }
}
