package experiments;


import traceTasks.VSMTraceBuilder;
import core.SparkTraceJob;
import core.SparkTraceTask;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static core.graphPipeline.SLayer.SGraph.syncSymbolValues;

public class DirtyBitExperiment extends SparkTraceJob {
    Dataset<Row> sourceDataset, targetDataset;
    public static String DIRTY_BIT_COL = "dirty_bit";
    String sourceIdCol = "commit_id";
    String targetIdCol = "issue_id";

    public DirtyBitExperiment(String sourcePath, String targetPath, double sourceDirtPercent, double targetDirtPercent) throws IOException {
        super("local[4]", "DirtyBit Experiment");//commit - issue
        sparkSession.sparkContext().setCheckpointDir("tmp");
        sourceDataset = prepareData(sourcePath, sourceDirtPercent).cache();
        targetDataset = prepareData(targetPath, targetDirtPercent).cache();
    }

    private Dataset<Row> prepareData(String filePath, double dirtyPercent) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        Path outPath = Paths.get("tmp", UUID.randomUUID().toString());
        List<String> appendedRows = new ArrayList<>();
        long dirtyRowNum = (long) (lines.size() * dirtyPercent);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if(i==0) {
                line+= String.format(",%s", DIRTY_BIT_COL);
            }else{
                boolean isDirty = i<dirtyRowNum;
                line+= String.format(",%s", String.valueOf(isDirty));
            }
            appendedRows.add(line);
        }
        Files.write(outPath, appendedRows);
        Dataset dataset =  sparkSession.read().option("header", "true").csv(outPath.toString());
        return dataset;
    }

    public long run(boolean useDirtyBit) throws Exception {
        long startTime = System.currentTimeMillis();
        SparkTraceTask vsmTask = new VSMTraceBuilder().getTask(sourceIdCol, targetIdCol);
        vsmTask.setUseDirtyBit(useDirtyBit);
        Map<String, String> config = new HashMap<>();
        config.put(VSMTraceBuilder.INPUT_TEXT1, "commit_content");
        config.put(VSMTraceBuilder.INPUT_TEXT2, "issue_content");
        vsmTask.setConfig(config);
        syncSymbolValues(vsmTask);
        vsmTask.train(sourceDataset, targetDataset, null);
        vsmTask.trace(sourceDataset, targetDataset).count();
        return System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) throws Exception {
        String sourcePath = "src/main/resources/maven_sample/commits.csv";
        String targetPath = "src/main/resources/maven_sample/improvement.csv";
        DirtyBitExperiment exp = new DirtyBitExperiment(sourcePath, targetPath, 1, 1);
        long nonDirtyBitRuntime = exp.run(true);
        System.out.println(String.format("DirtyBit Running Time = %d", nonDirtyBitRuntime));
    }
}