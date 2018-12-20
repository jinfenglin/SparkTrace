import core.SparkTraceJob;
import examples.TestBase;
import featurePipeline.CosinSimilarityStage;
import featurePipeline.DDFPipeline;
import featurePipeline.SDFPipeline;
import featurePipeline.TraceModelPipeline;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenLink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SparkJobTest extends TestBase {
    private static String masterUrl = "local";

    public SparkJobTest() {
        super(masterUrl);
    }

    @Test
    public void runSparkTestWithMavenData() {
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        String linkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        Dataset<MavenCommit> commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        Dataset<MavenImprovement> improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        Dataset<MavenLink> links = TraceDatasetFactory.createDatasetFromCSV(sparkSession, linkPath, MavenLink.class);


    }

    @Test
    public void multiDuplicatedStagePerformanceTest() {
        Dataset<Row> dataset = getSentenceDataset();
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
}
