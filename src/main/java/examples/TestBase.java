package examples;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import traceability.BasicTraceArtifact;
import traceability.BasicTraceLink;
import traceability.TraceDatasetFactory;


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
}
