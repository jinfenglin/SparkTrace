package traceability;


import org.apache.spark.sql.*;
import traceability.components.abstractComponents.TraceArtifact;
import traceability.components.abstractComponents.TraceLink;

import java.util.List;


public class TraceDatasetFactory {
    protected TraceDatasetFactory() {

    }

    //Create a dataset from a collection of java objects
    public static <T> Dataset<T> createDataset(SparkSession sparkSession, List<T> artifactCollections, Class<T> beanClass) {
        Encoder encoder = Encoders.bean(beanClass);
        return sparkSession.createDataset(artifactCollections, encoder);
    }

    //Create dataset from different formats of files directly
    public static <T> Dataset<T> createDatasetFromCSV(SparkSession sparkSession, String csvPath, Class<T> beanClass) {
        Encoder encoder = Encoders.bean(beanClass);
        Dataset<Row> dataFrame = sparkSession.read().option("header", "true").csv(csvPath);
        return dataFrame.as(encoder);
    }

    public static <T extends TraceArtifact> Dataset<T> createArtifacts(SparkSession session, List<T> artifacts, Class<T> bean) {
        return createDataset(session, artifacts, bean);
    }

    public static <T extends TraceLink> Dataset<T> createLinks(SparkSession session, List<T> links, Class<T> bean) {
        return createDataset(session, links, bean);
    }

    public static <T extends TraceArtifact> Dataset<T> createArtifactsFromCSV(SparkSession session, String csvPath, Class<T> bean) {
        return createDatasetFromCSV(session, csvPath, bean);
    }

    public static <T extends TraceLink> Dataset<T> createLinksFromCSV(SparkSession session, String csvPath, Class<T> bean) {
        return createDatasetFromCSV(session, csvPath, bean);
    }

}
