package experiments;


import buildingBlocks.traceTasks.VSMTraceBuilder;
import core.SparkTraceJob;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.*;

public class DirtyBitExperiment extends SparkTraceJob {
    Dataset<Row> sourceDataset, targetDataset;
    public static String DIRTY_BIT_COL = "dirty_bit";
    String sourceIdCol = "commit_id";
    String targetIdCol = "issue_id";

    public DirtyBitExperiment(String sourcePath, String targetPath, double sourceDirtPercent, double targetDirtPercent) {
        super("local[4]", "DirtyBit Experiment");//commit - issue
        sparkSession.sparkContext().setCheckpointDir("tmp");
        sourceDataset = sparkSession.read().option("header", "true").csv(sourcePath); //commit
        targetDataset = sparkSession.read().option("header", "true").csv(targetPath); //issue
        sourceDataset.select(sourceIdCol, new String[]{"commit_content"});
        targetDataset.select(targetIdCol, new String[]{"issue_content"});
        sourceDataset = prepareData(sourceDataset, sourceIdCol, sourceDirtPercent).checkpoint();
        targetDataset = prepareData(targetDataset, targetIdCol, targetDirtPercent).checkpoint();
    }

    private Dataset prepareData(Dataset dataset, String datasetIdCol, double dirtyPercent) {
        dataset = dataset.orderBy(rand());
        Dataset selectedRows = dataset.select(datasetIdCol).limit((int) (dataset.count() * dirtyPercent)).withColumn(DIRTY_BIT_COL, lit(true));
        dataset = dataset.orderBy(rand());
        Seq<String> colNames = scala.collection.JavaConverters.asScalaIteratorConverter(
                Arrays.asList(datasetIdCol).iterator()
        ).asScala().toSeq();
        dataset = dataset.join(selectedRows, colNames, "left_outer");
        dataset = dataset.na().fill(false, new String[]{DIRTY_BIT_COL});
        return dataset;
    }

    public long run() throws Exception {
        long startTime = System.currentTimeMillis();
        SparkTraceTask vsmTask = new VSMTraceBuilder().getTask(sourceIdCol, targetIdCol);
        vsmTask.setUseDirtyBit(false);
        Map<String, String> config = new HashMap<>();
        config.put(VSMTraceBuilder.INPUT_TEXT1, "commit_content");
        config.put(VSMTraceBuilder.INPUT_TEXT2, "issue_content");
        vsmTask.setConfig(config);
        syncSymbolValues(vsmTask);
        vsmTask.train(sourceDataset, targetDataset, null);
        vsmTask.trace(sourceDataset, targetDataset);
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
