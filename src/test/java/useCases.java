import examples.TestBase;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenICLink;

import static org.apache.spark.sql.functions.*;

/**
 *
 */
public class useCases extends TestBase {

    public useCases() {
        super("local");
    }

    @Test
    public void mergeEffectiveTest() {
        Dataset<MavenCommit> commits;
        Dataset<MavenImprovement> improvements;
        Dataset<MavenICLink> links;
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        String linkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        links = TraceDatasetFactory.createDatasetFromCSV(sparkSession, linkPath, MavenICLink.class);
        Dataset<Row> df = commits.toDF();
        Pipeline pipeline = new Pipeline();
        int num = 1;
        PipelineStage[] stage = new PipelineStage[num];
        stage[0] = new Tokenizer().setInputCol("commit_content").setOutputCol("token");
        for (int i = 1; i < num; i++) {
            stage[i] = new HashingTF().setInputCol("token");

        }
        pipeline.setStages(stage);
        PipelineModel model = pipeline.fit(df);
        long tStart = System.currentTimeMillis();
        df = model.transform(df);
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        df.explain(true);
        System.out.println("time=" + elapsedSeconds);

    }

    @Test
    public void FeaturesCreation() {
        /**
         * multiple features may share some common processing component. etc Closest previous linked commit
         * Closest subsequent linked commit
         */

    }

    @Test
    public void votingSystem() {
        /**
         *
         */

    }

    @Test
    public void MultiTextualFeature() {
        /**
         * Processing different type of textual features, VSM, LDA etc.
         */
    }

    @Test
    public void nestedTraceModelAsFeature() {
        /**
         * use an existing trace model directly. Resolve a trace model into current graph
         */
    }


    @Test
    public void experiment() {
        Dataset<Row> d0 = getSentenceLabelDataset();
        Dataset<Row> d2 = d0.withColumn("words", split(col("sentence"), "\\s+"));
        Dataset<Row> d3 = d0.withColumnRenamed("label", "label1").withColumnRenamed("sentence","sentence1");

        Dataset<Row> d4 = d2.crossJoin(d3);
        d4.cache();
        Dataset<Row> d5 = d4.select("words");
        Dataset<Row> d6 = d4.select("label");
        d5.explain();
        d6.explain();
    }


}
