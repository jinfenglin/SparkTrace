import examples.TestBase;
import org.apache.spark.sql.Dataset;
import org.junit.Test;

import traceability.TraceDatasetFactory;
import traceability.components.abstractComponents.TraceComponent;
import traceability.components.basic.BasicTraceLink;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenICLink;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DatasetCreationTest extends TestBase {
    private static String masterUrl = "local";
    private static int CREATION_NUM = 10;
    //The small dataset of maven
    private static int MAVEN_COMMIT_NUM = 4803, MAVEN_IMPROVEMENT_NUM = 230, MAVEN_LINK_NUM = 159;


    public DatasetCreationTest() {
        super(masterUrl);
    }

    /**
     * Read data from a file and verify the dataset is constructed with proper number of objects.
     *
     * @param filePath  The path of file provide data
     * @param objectNum The number of objects that should be created
     */
    public void readCSVDataAndVerifyNum(String filePath, int objectNum, Class<? extends TraceComponent> bean) {
        Dataset dateset = TraceDatasetFactory.createDatasetFromCSV(sparkSession, filePath, bean);
        List componetList = dateset.collectAsList();
        assertEquals(componetList.size(), objectNum);
    }

    @Test
    public void verifyMavenDatasetCreatedThoughCSVReader() {
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        readCSVDataAndVerifyNum(commitPath, MAVEN_COMMIT_NUM, MavenCommit.class);

        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        readCSVDataAndVerifyNum(improvementPath, MAVEN_IMPROVEMENT_NUM, MavenImprovement.class);

        String linkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        readCSVDataAndVerifyNum(linkPath, MAVEN_LINK_NUM, MavenICLink.class);
    }

    @Test
    public void verifyLinkDatasetCreated() {
        Dataset<BasicTraceLink> linkDataset = getBasicTraceLinks(CREATION_NUM);
        List<BasicTraceLink> linkList = linkDataset.collectAsList();
        assertEquals(linkList.size(), CREATION_NUM);
    }
}
