package examples;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import traceability.components.basic.BasicTraceArtifact;
import traceability.components.basic.BasicTraceLink;
import traceability.TraceDatasetFactory;


import javax.xml.crypto.Data;
import java.util.*;

public class TestBase {
    protected SparkSession sparkSession;

    public TestBase(String masterUrl) {
        String jobName = "SparkTest";
        SparkConf conf = new SparkConf();
        conf.setMaster(masterUrl);
        conf.setAppName(jobName);
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Create N BasicArtifacts and put them into a dataset
     *
     * @param n
     * @return
     */
    public Dataset<BasicTraceArtifact> getBasicTraceArtifacts(int n) {
        List<BasicTraceArtifact> basicArtifacts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = "artifact_" + String.valueOf(i);
            BasicTraceArtifact basicArtifact = new BasicTraceArtifact(id);
            basicArtifacts.add(basicArtifact);
        }
        return TraceDatasetFactory.createArtifacts(sparkSession, basicArtifacts, BasicTraceArtifact.class);
    }

    public Dataset<BasicTraceLink> getBasicTraceLinks(int n) {
        List<BasicTraceLink> basicTraceLinks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String sourceId = "artifact_" + String.valueOf(i);
            String targetId = "artifact_" + String.valueOf(i);
            String label = "0.0";
            BasicTraceLink basicTraceLink = new BasicTraceLink(sourceId, targetId, label);
            basicTraceLinks.add(basicTraceLink);
        }
        return TraceDatasetFactory.createLinks(sparkSession, basicTraceLinks, BasicTraceLink.class);
    }

    public Dataset<Row> getSentenceDataset() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "Welcome to TutorialKart."),
                RowFactory.create(0.0, "Learn Spark at TutorialKart."),
                RowFactory.create(1.0, "Spark Mllib has TF-IDF.")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        // import data with the schema
        Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);
        return sentenceData;
    }
}
