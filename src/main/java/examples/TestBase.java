package examples;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import traceability.Artifact;
import traceability.BasicArtifact;
import traceability.TraceDatasetFactory;


import java.sql.Timestamp;
import java.util.*;

public class TestBase {
    SparkSession sparkSession;


    public  TestBase(String masterUrl) {
        String jobName = "SparkTest";
        SparkConf conf = new SparkConf();
        conf.setMaster(masterUrl);
        conf.setAppName(jobName);
        sparkSession = SparkSession.builder().config(conf).getOrCreate();

    }

    public List<Artifact> getArtifacts() {
        BasicArtifact a1 = new BasicArtifact("a1");
        BasicArtifact a2 = new BasicArtifact("a2");
        List<BasicArtifact> basicArtifacts = new ArrayList<>();
        basicArtifacts.add(a1);
        basicArtifacts.add(a2);

        Dataset<BasicArtifact> dataset = TraceDatasetFactory.createArtifacts(sparkSession, basicArtifacts, BasicArtifact.class);
        dataset.collectAsList();
        return Arrays.asList(a1, a2);
    }

    public static void main(String[] args) {
        TestBase testBase = new TestBase("local");
        testBase.getArtifacts();

    }
}
