package traceability;


import org.apache.spark.sql.*;
import java.util.List;


public class TraceDatasetFactory {
    protected TraceDatasetFactory() {

    }

    public static <T> Dataset<T> createDataset(SparkSession sparkSession, List<T> artifactCollections, Class<T> beanClass) {
        Encoder encoder = Encoders.bean(beanClass);
        return sparkSession.createDataset(artifactCollections, encoder);

    }

    public static <T extends TraceArtifact> Dataset<T> createArtifacts(SparkSession session, List<T> artifacts, Class<T> bean) {
        return createDataset(session, artifacts, bean);
    }

    public static <T extends TraceLink> Dataset<T> createLinks(SparkSession session, List<T> links, Class<T> bean) {
        return createDataset(session, links, bean);
    }
}
