package experiments;


import buildingBlocks.traceTasks.VSMTraceBuilder;
import core.SparkTraceJob;
import core.SparkTraceTask;
import org.apache.spark.sql.*;
import scala.collection.Seq;

import java.io.IOException;
import java.util.*;

import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.*;

public class DirtyBitExperiment extends SparkTraceJob {
    Dataset<Row> sourceDataset, targetDataset;
    public static String DIRTY_BIT_COL = "dirty_bit";
    String sourceIdCol = "commit_id";
    String targetIdCol = "issue_id";

    public DirtyBitExperiment(String sourcePath, String targetPath, double sourceDirtPercent, double targetDirtPercent) throws IOException {
        super("local[4]", "DirtyBit Experiment");//commit - issue
        sparkSession.sparkContext().setCheckpointDir("tmp");
        sourceDataset = sparkSession.read().option("header", "true").csv(sourcePath); //commit
        targetDataset = sparkSession.read().option("header", "true").csv(targetPath); //issue
        sourceDataset.select(sourceIdCol, new String[]{"commit_content"});
        targetDataset.select(targetIdCol, new String[]{"issue_content"});
        sourceDataset = prepareData(sourceDataset, sourceIdCol, sourceDirtPercent).cache();
        targetDataset = prepareData(targetDataset, targetIdCol, targetDirtPercent).cache();
    }

    private Dataset prepareData(Dataset dataset, String datasetIdCol, double dirtyPercent) {
        long size = dataset.count();
        dataset = dataset.orderBy(rand());
        Dataset selectedRows = dataset.select(datasetIdCol).limit((int) (size * dirtyPercent)).withColumn(DIRTY_BIT_COL, lit(true));
        dataset = dataset.orderBy(rand());
        Seq<String> colNames = scala.collection.JavaConverters.asScalaIteratorConverter(
                Arrays.asList(datasetIdCol).iterator()
        ).asScala().toSeq();
        dataset = dataset.join(selectedRows, colNames, "left_outer");
        dataset = dataset.na().fill(false, new String[]{DIRTY_BIT_COL});
        return dataset;
    }

    public long run() throws Exception {
        SparkTraceTask vsmTask = new VSMTraceBuilder().getTask(sourceIdCol, targetIdCol);
        vsmTask.setUseDirtyBit(true);
        Map<String, String> config = new HashMap<>();
        config.put(VSMTraceBuilder.INPUT_TEXT1, "commit_content");
        config.put(VSMTraceBuilder.INPUT_TEXT2, "issue_content");
        vsmTask.setConfig(config);
        syncSymbolValues(vsmTask);
        vsmTask.train(sourceDataset, targetDataset, null);
        long startTime = System.currentTimeMillis();
        Dataset result = vsmTask.trace(sourceDataset, targetDataset);
        result.count();
        return System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) throws Exception {
        String sourcePath = "src/main/resources/maven_sample/commits.csv";
        String targetPath = "src/main/resources/maven_sample/improvement.csv";
        DirtyBitExperiment exp = new DirtyBitExperiment(sourcePath, targetPath, 0.2, 0.1);
        long runtime = exp.run();
        System.out.println(String.format("Running Time = %d", runtime));
    }
}
