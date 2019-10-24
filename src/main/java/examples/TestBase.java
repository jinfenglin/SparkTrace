package examples;


import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import traceability.components.basic.BasicTraceArtifact;
import traceability.components.basic.BasicTraceLink;
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

    public Dataset<Row> getSentenceLabelDataset() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "Welcome to TutorialKart."),
                RowFactory.create(0.0, "Learn Spark at TutorialKart."),
                RowFactory.create(1.0, "Spark Mllib has TF-IDF.")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, true, Metadata.empty())
        });

        // import data with the schema
        Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);
        return sentenceData;
    }

    public Dataset<Row> getSentenceDataset(List<String> sentence) {
        List<Row> data = new ArrayList<>();
        sentence.forEach(s -> data.add(RowFactory.create(s)));
        StructType schema = new StructType(new StructField[]{
                new StructField("text", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);
        return sentenceData;
    }

    public Dataset<Row> getMultiSentenceRowData() {
        List<Row> data = Arrays.asList(
                RowFactory.create(null, "Welcome to TutorialKart."),
                RowFactory.create(null, "Learn Spark at TutorialKart."),
                RowFactory.create(null, "Spark Mllib has TF-IDF."),
                RowFactory.create("Spark SQL and DataFrames - Spark 1.6.0 Documentation", null),
                RowFactory.create("How to print unique values of a column of DataFrame in Spark", null)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("text1", DataTypes.StringType, true, Metadata.empty()),
                new StructField("text2", DataTypes.StringType, true, Metadata.empty())
        });

        // import data with the schema
        Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);
        return sentenceData;
    }

    public void printPipeline(Pipeline pipeline) {
        PipelineStage[] stages = pipeline.getStages();
        for (PipelineStage stage : stages) {
            if (stage instanceof Pipeline) {
                Pipeline subPipeline = (Pipeline) stage;
                printPipeline(subPipeline);
            } else {
                System.out.println("==========================");
                System.out.println(stage);
                System.out.println(stage.explainParams());
            }
        }
    }

    public Dataset<Row> createNullDataset() {
        List<Row> data = Arrays.asList(
                RowFactory.create(null, null),
                RowFactory.create(null, null),
                RowFactory.create(null, null)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, true, Metadata.empty())
        });
        return sparkSession.createDataFrame(data, schema);
    }

    public Dataset<Row> createDatasetWithNull() {
        Dataset<Row> data = getSentenceLabelDataset();
        Dataset<Row> nulls = createNullDataset();
        return data.union(nulls);
    }

    protected Map<String, String> getVSMTaskConfig() {
        Map<String, String> vsmTaskInputConfig = new HashMap<>();
        vsmTaskInputConfig.put("s_text", "commit_content");
        vsmTaskInputConfig.put("t_text", "issue_content");
        vsmTaskInputConfig.put("s_id", "commit_id");
        vsmTaskInputConfig.put("t_id", "issue_id");
        return vsmTaskInputConfig;
    }

}
