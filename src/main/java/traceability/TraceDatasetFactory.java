package traceability;


import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class TraceDatasetFactory {
    protected TraceDatasetFactory() {

    }

    public static <T> Dataset<T> createDataset(SparkSession sparkSession, List<T> artifactCollections, Class<T> beanClass) {
        Encoder encoder = Encoders.bean(beanClass);
        return sparkSession.createDataset(artifactCollections, encoder);

    }

    public static <T extends Artifact> Dataset<T> createArtifacts(SparkSession session, List<T> artifacts, Class<T> bean) {
        return createDataset(session, artifacts, bean);
    }


}
