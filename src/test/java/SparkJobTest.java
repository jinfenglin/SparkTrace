import core.SparkTraceJob;
import examples.TestBase;
import featurePipeline.CosinSimilarityStage;
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
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenLink;

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

        //Merge the common part of commit and issue to enable vsm training on larger dataset
        SparkTraceJob job = new SparkTraceJob(sparkSession);
        SparkTraceJob fooJob = new SparkTraceJob(sparkSession);
        SDFPipeline commitPipeline = new SDFPipeline("commit_id");
        Tokenizer commitTokenizer = new Tokenizer().setInputCol("content").setOutputCol("commit_token");
        HashingTF commitHashingTF = new HashingTF().setInputCol(commitTokenizer.getOutputCol()).setOutputCol("commit_tf");
        IDF commitIdf = new IDF().setInputCol(commitHashingTF.getOutputCol()).setOutputCol("commit_tfidf_vec");
        commitPipeline.setPipelineStages(new PipelineStage[]{commitTokenizer, commitHashingTF, commitIdf});
        job.setSourceSDFPipeline(commitPipeline);

        SDFPipeline improvementSDFPipeline = new SDFPipeline("issue_id");
        Tokenizer tokenizer = new Tokenizer().setInputCol("content").setOutputCol("issue_token");
        HashingTF hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol()).setOutputCol("issue_tf");
        IDF idf = new IDF().setInputCol(hashingTF.getOutputCol()).setOutputCol("issue_tfidf_vec");
        improvementSDFPipeline.setPipelineStages(new PipelineStage[]{tokenizer, hashingTF, idf});
        job.setTargetSDFPipeline(improvementSDFPipeline);


        DDFPipeline dualArtifactFeatures = new DDFPipeline("commit_id", "issue_id");
        CosinSimilarityStage cosinSimilarityStage = new CosinSimilarityStage().setInputCol1("commit_tfidf_vec").setInputCol2("issue_tfidf_vec").setOutputCol("score");
        dualArtifactFeatures.setPipelineStages(new PipelineStage[]{cosinSimilarityStage});
        job.setDdfPipeline(dualArtifactFeatures);

        TraceModelPipeline modelPipeline = new TraceModelPipeline("features");
        job.setModelPipeline(modelPipeline);

        job.trainSupervisedModel(commits, improvements, links);
        job.trace(commits, improvements);
    }
}
