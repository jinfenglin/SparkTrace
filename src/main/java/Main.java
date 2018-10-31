import examples.TestBase;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import traceability.Artifact;

import java.util.List;




public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Example");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Encoder<Artifact> artifactEncoder = Encoders.bean(Artifact.class);
        TestBase tb = new TestBase();
        List<Artifact> artifacts = tb.getArtifacts();

        Dataset<Artifact> ds = sparkSession.createDataset(artifacts, artifactEncoder);
        ds.show();
        Dataset<Row> ads = ds.select("attributes");
        ads.show();

    }
}
