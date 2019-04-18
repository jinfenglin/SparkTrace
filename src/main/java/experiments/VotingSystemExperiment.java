package experiments;

import traceTasks.LDATraceBuilder;
import traceTasks.NGramVSMTraceTaskBuilder;
import traceTasks.OptimizedVoteTraceBuilder;
import traceTasks.VSMTraceBuilder;
import core.SparkTraceJob;

import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import core.SparkTraceTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.Seq;
import org.apache.hadoop.conf.Configuration;

import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.col;

//        Path outputPath = new Path("gs://dataflow-exp1/google_storage_tests/20170524/outputfolder/Test.csv");
//        OutputStream out = outputPath.getFileSystem(new Configuration()).create(outputPath);

public class VotingSystemExperiment extends SparkTraceJob {
    Dataset<Row> sourceDataset, targetDataset;
    String sourceId = "s_id", targetId = "t_id";

    public VotingSystemExperiment(String sourcePath, String targetPath) {
        super("local[4]", "Voting system");
        sourceDataset = sparkSession.read().option("header", "true").csv(sourcePath).cache(); //commit
        targetDataset = sparkSession.read().option("header", "true").csv(targetPath).cache(); //issue
    }

    private Map<String, String> getConfig() {
        Map<String, String> vsmTaskInputConfig = new HashMap<>();
        vsmTaskInputConfig.put("s_text", " commit_diff");
        vsmTaskInputConfig.put("t_text", "issue_content");
        vsmTaskInputConfig.put("s_id", "commit_id");
        vsmTaskInputConfig.put("t_id", "issue_id");
        return vsmTaskInputConfig;
    }

    public long runOptimizedSystem(String outputDir) throws Exception {
        long startTime = System.currentTimeMillis();
        SparkTraceTask voteTask = new OptimizedVoteTraceBuilder().getTask(sourceId, targetId);
        Map<String, String> config = getConfig();

        sourceDataset = sourceDataset.filter(col(config.get("s_text")).isNotNull()).cache();
        targetDataset = targetDataset.filter(col(config.get("t_text")).isNotNull()).cache();
        voteTask.setConfig(config);
        voteTask.getSourceSDFSdfGraph().optimize(voteTask.getSourceSDFSdfGraph());
        voteTask.getTargetSDFSdfGraph().optimize(voteTask.getTargetSDFSdfGraph());
        syncSymbolValues(voteTask);
        voteTask.train(sourceDataset, targetDataset, null);
        Dataset<Row> result = voteTask.trace(sourceDataset, targetDataset);

        String vsmScoreCol = voteTask.getOutputField(OptimizedVoteTraceBuilder.VSM_SCORE).getFieldSymbol().getSymbolValue();
        String ngramVsmScoreCol = voteTask.getOutputField(OptimizedVoteTraceBuilder.NGRAM_SCORE).getFieldSymbol().getSymbolValue();
        String ldaScoreCol = voteTask.getOutputField(OptimizedVoteTraceBuilder.LDA_SCORE).getFieldSymbol().getSymbolValue();
        result.select(config.get(sourceId), config.get(targetId), vsmScoreCol, ngramVsmScoreCol, ldaScoreCol).write()
                .format("com.databricks.spark.csv")
                .option("header", "true").mode("overwrite")
                .save(outputDir + "/voteResult_op.csv");
        return System.currentTimeMillis() - startTime;
    }

    public long runUnOptimized(String outputDir) throws Exception {
        long startTime = System.currentTimeMillis();
        SparkTraceTask vsmTask = new VSMTraceBuilder().getTask(sourceId, targetId);
        vsmTask.setCleanColumns(false);
        Map<String, String> vsmTaskInputConfig = getConfig();
        String s_id_col_name = vsmTaskInputConfig.get(sourceId);
        String t_id_col_name = vsmTaskInputConfig.get(targetId);
        sourceDataset = sourceDataset.filter(col(vsmTaskInputConfig.get("s_text")).isNotNull()).cache();
        targetDataset = targetDataset.filter(col(vsmTaskInputConfig.get("t_text")).isNotNull()).cache();
        vsmTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(vsmTask);
        vsmTask.train(sourceDataset, targetDataset, null);
        Dataset<Row> result1 = vsmTask.trace(sourceDataset, targetDataset);

        SparkTraceTask ngramTask = new NGramVSMTraceTaskBuilder().getTask(sourceId, targetId);
        ngramTask.setCleanColumns(false);
        ngramTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(ngramTask);
        ngramTask.train(sourceDataset, targetDataset, null);
        Dataset<Row> result2 = ngramTask.trace(sourceDataset, targetDataset);

        SparkTraceTask ldaTask = new LDATraceBuilder().getTask(sourceId, targetId);
        ldaTask.setCleanColumns(false);
        ldaTask.setConfig(vsmTaskInputConfig);
        syncSymbolValues(ldaTask);
        ldaTask.train(sourceDataset, targetDataset, null);
        Dataset<Row> result3 = ldaTask.trace(sourceDataset, targetDataset);

        String vsmScoreCol = vsmTask.getOutputField(VSMTraceBuilder.OUTPUT).getFieldSymbol().getSymbolValue();
        String ngramVsmScoreCol = ngramTask.getOutputField(NGramVSMTraceTaskBuilder.OUTPUT).getFieldSymbol().getSymbolValue();
        String ldaScoreCol = ldaTask.getOutputField(LDATraceBuilder.OUTPUT).getFieldSymbol().getSymbolValue();

        Seq<String> colNames = scala.collection.JavaConverters.asScalaIteratorConverter(
                Arrays.asList(s_id_col_name, t_id_col_name).iterator()
        ).asScala().toSeq();
        Dataset<Row> result = result1.join(result2, colNames);
        result = result.join(result3, colNames);
        result.select(vsmTaskInputConfig.get(sourceId), vsmTaskInputConfig.get(targetId), vsmScoreCol, ngramVsmScoreCol, ldaScoreCol).write()
                .format("com.databricks.spark.csv")
                .option("header", "true").mode("overwrite")
                .save(outputDir + "/voteResult_unOP.csv");
        return System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) throws Exception {
        //System.setProperty("hadoop.home.dir", "G:\\tools\\spark-2.4.0-bin-hadoop2.7");
        String dataDirRoot = args[0]; //"src/main/resources/git_projects"
        String outputDir = args[1]; // "results"
        List<String> projects = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            projects.add(args[i]);
        }
        //projects.addAll(Arrays.asList(new String[]{"alibaba/ARouter", "alibaba/arthas", "alibaba/canal", "alibaba/rax", "baidu/san", "meituan/EasyReact", "Tencent/bk-cmdb"}));
        projects.addAll(Arrays.asList(new String[]{"meituan/EasyReact"}));
        List<String> opTimeList = new ArrayList<>();
        List<String> unOpTimeList = new ArrayList<>();
        for (String projectPath : projects) {
            Path dataDir = Paths.get(dataDirRoot, projectPath, "translated_data", "clean_translated_tokens");
            Path sourceArtifactPath = Paths.get(dataDir.toString(), "commit.csv");
            Path targetArtifactPath = Paths.get(dataDir.toString(), "issue.csv");
            VotingSystemExperiment vs = new VotingSystemExperiment(sourceArtifactPath.toString(), targetArtifactPath.toString());
            long opTime = vs.runOptimizedSystem(outputDir);
            long unOpTime = vs.runUnOptimized(outputDir);
            opTimeList.add(String.valueOf(opTime));
            unOpTimeList.add(String.valueOf(unOpTime));
            System.out.println(String.format("%s Time =  %d vs %d", projectPath, opTime, unOpTime));
        }
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputDir + "/time.csv");
        OutputStream out = outputPath.getFileSystem(new Configuration()).create(outputPath);
        String projectNameLine = String.join(",", projects) + "\n";
        String opTimeLine = String.join(",", opTimeList) + "\n";
        String unOpTimeLine = String.join(",", unOpTimeList) + "\n";
        out.write(projectNameLine.getBytes());
        out.write(opTimeLine.getBytes());
        out.write(unOpTimeLine.getBytes());
        out.close();
    }
}
