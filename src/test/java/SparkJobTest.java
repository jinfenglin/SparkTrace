import core.SparkTraceJob;
import examples.TestBase;
import featurePipeline.DDFPipeline;
import featurePipeline.SDFPipeline;
import featurePipeline.TraceModelPipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.junit.Test;
import traceability.TraceDatasetFactory;
import traceability.components.basic.BasicTraceLink;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenLink;

import java.util.List;

public class SparkJobTest extends TestBase {
    private static String masterUrl = "local";

    public SparkJobTest() {
        super(masterUrl);
    }

    @Test
    public void runSparkTestWithMavenData() {
        String commitPath = "src/main/resources/maven_mini/commits.csv";
        String improvementPath = "src/main/resources/maven_mini/improvement.csv";
        String linkPath = "src/main/resources/maven_mini/improvementCommitLinks.csv";
        Dataset<MavenCommit> commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        Dataset<MavenImprovement> improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        Dataset<MavenLink> links = TraceDatasetFactory.createDatasetFromCSV(sparkSession, linkPath, MavenLink.class);

        //Merge the common part of commit and issue to enable vsm training on larger dataset
        SparkTraceJob job = new SparkTraceJob(sparkSession);
        SDFPipeline commitPipeline = new SDFPipeline("id");
        Tokenizer tokenizer = new Tokenizer().setInputCol("content").setOutputCol("content_token");
        HashingTF hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol()).setOutputCol("content_tf");
        IDF idf = new IDF().setInputCol(hashingTF.getOutputCol()).setOutputCol("content_tfidf_vec");
        commitPipeline.setPipelineStages(new PipelineStage[]{tokenizer, hashingTF, idf});
        job.setSourceSDFPipelien(commitPipeline);

        SDFPipeline improvementSDFPipeline = new SDFPipeline("id");
        improvementSDFPipeline.setPipelineStages(new PipelineStage[]{tokenizer, hashingTF, idf});
        job.setTargetSDFPipeline(improvementSDFPipeline);


        DDFPipeline dualArtifactFeatures = new DDFPipeline("commit_id", "issue_id");
        job.setDdfPipeline(dualArtifactFeatures);

        TraceModelPipeline modelPipeline = new TraceModelPipeline("features");
        job.setModelPipeline(modelPipeline);

        job.trainSupervisedModel(commits, improvements, links);
        List<BasicTraceLink> scoredLinks = job.trace(commits, improvements);
    }
}
