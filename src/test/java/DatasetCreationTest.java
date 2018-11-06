import examples.TestBase;
import org.apache.spark.sql.Dataset;
import org.junit.Test;

import traceability.BasicTraceArtifact;
import traceability.BasicTraceLink;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DatasetCreationTest extends TestBase {
    private static String masterUrl = "local";
    private int CREATION_NUM = 10;

    public DatasetCreationTest() {
        super(masterUrl);
    }

    @Test
    public void verifyArtifactDatasetCreated() {
        Dataset<BasicTraceArtifact> artifactDataset = getBasicTraceArtifacts(CREATION_NUM);
        List<BasicTraceArtifact> artifactList = artifactDataset.collectAsList();
        assertEquals(artifactList.size(), CREATION_NUM);
    }

    @Test
    public void verifyLinkDatasetCreated() {
        Dataset<BasicTraceLink> linkDataset = getBasicTraceLinks(CREATION_NUM);
        List<BasicTraceLink> linkList = linkDataset.collectAsList();
        assertEquals(linkList.size(), CREATION_NUM);
    }
}
