package experiments;

import buildingBlocks.preprocessor.NGramCount;
import buildingBlocks.traceTasks.NGramVSMTraceTask;
import buildingBlocks.traceTasks.VSMTraceBuilder;
import buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceJob;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.col;

/**
 *
 */
public class Maven3ArtifactsExperiment extends SparkTraceJob {
    Dataset<Row> code, bug, commit;
    String CODE_ID = "code_id", BUG_ID = "bug_id", COMMIT_ID = "commit_id";
    String codeContent = "code_content", commitContent = "commit_content", bugContent = "bug_content";
    String codeContentHTF = "code_htf", commitContentHTF = "commit_htf", bugContentHTF = "bug_htf";
    String outputDir = "tmp";
    SGraph bugPreprocess, codePreprocess, commitPreprocess;

    public Maven3ArtifactsExperiment(String codeFilePath, String bugPath, String commitPath, String sourceCodeRootDir) throws IOException {
        super("local[4]", "Maven Cross Trace");
        bug = sparkSession.read().option("header", "true").csv(bugPath);
        bug = bug.where(col(bugContent).isNotNull());
        commit = sparkSession.read().option("header", "true").csv(commitPath);
        code = readCodeFromFile(codeFilePath, sourceCodeRootDir);
    }

    private Map<String, String> getConfig() {
        Map<String, String> config = new HashMap<>();
        return config;
    }

    private Dataset<Row> readCodeFromFile(String codeFilePath, String codeDirPath) throws IOException {
        File codeIdFile = new File(codeFilePath);
        List<String> lines = Files.readAllLines(codeIdFile.toPath());
        lines.remove(0);
        List<Row> rows = new ArrayList<>();
        for (String codeId : lines) {
            Path codeFile = Paths.get(codeDirPath, codeId.split(",")[0]);
            String content = new String(Files.readAllBytes(codeFile));
            rows.add(RowFactory.create(codeId, content));
        }
        StructType schema = new StructType(new StructField[]{
                new StructField(CODE_ID, DataTypes.StringType, true, Metadata.empty()),
                new StructField(codeContent, DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> sentenceData = sparkSession.createDataFrame(rows, schema);
        return sentenceData;
    }

    //step 1
    private List<Dataset> preprocess(Dataset code, Dataset commit, Dataset bug) throws Exception {
        List<Dataset> dataSets = new ArrayList<>();
        bugPreprocess = NGramCount.getGraph("commit_preprocess");
        codePreprocess = NGramCount.getGraph("code_preprocess");
        commitPreprocess = NGramCount.getGraph("commit_process");

        Map<String, String> codeConfig = new HashMap<>();
        Map<String, String> bugConfig = new HashMap<>();
        Map<String, String> commitConfig = new HashMap<>();

        codeConfig.put(NGramCount.INPUT_TEXT_COL, codeContent);
        commitConfig.put(NGramCount.INPUT_TEXT_COL, commitContent);
        bugConfig.put(NGramCount.INPUT_TEXT_COL, bugContent);


        bugPreprocess.optimize(bugPreprocess);
        codePreprocess.optimize(codePreprocess);
        commitPreprocess.optimize(commitPreprocess);

        codePreprocess.setConfig(codeConfig);
        commitPreprocess.setConfig(commitConfig);
        bugPreprocess.setConfig(bugConfig);

        syncSymbolValues(codePreprocess);
        syncSymbolValues(commitPreprocess);
        syncSymbolValues(bugPreprocess);

        Dataset code_processed = codePreprocess.toPipeline().fit(code).transform(code);
        Dataset commit_processed = commitPreprocess.toPipeline().fit(commit).transform(commit);
        Dataset bug_processed = bugPreprocess.toPipeline().fit(bug).transform(bug);

        dataSets.add(code_processed);
        dataSets.add(commit_processed);
        dataSets.add(bug_processed);
        return dataSets;
    }

    //step2
    private List<PipelineModel> unsupervisedLearn(Dataset code, Dataset commit, Dataset bug) throws Exception {
        SGraph idf1 = IDFGraphPipeline.getGraph("code-commit");
        SGraph idf2 = IDFGraphPipeline.getGraph("commit-bug");
        SGraph idf3 = IDFGraphPipeline.getGraph("code-bug");

        String mixedInputCol = "tmpMixedCol";
        StructField field = DataTypes.createStructField(mixedInputCol, new VectorUDT(), false);
        StructType st = new StructType(new StructField[]{field});
        Dataset<Row> trainingData1 = code.sparkSession().createDataFrame(new ArrayList<>(), st);
        Dataset<Row> trainingData2 = code.sparkSession().createDataFrame(new ArrayList<>(), st);

        Dataset<Row> commitHtf = commit.select(commitPreprocess.getOutputField(NGramCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        Dataset<Row> bugHtf = bug.select(bugPreprocess.getOutputField(NGramCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());

        trainingData1 = trainingData1.union(commitHtf);
        trainingData2 = trainingData2.union(bugHtf);

        Pipeline pipe1 = idf1.toPipeline();
        Pipeline pipe2 = idf2.toPipeline();
        List<PipelineModel> models = new ArrayList<>();

        PipelineStage innerStage = pipe1.getStages()[0];
        String inputColParam = "inputCol";
        innerStage.set(inputColParam, mixedInputCol);
        pipe1.setStages(new PipelineStage[]{innerStage});
        models.add(pipe1.fit(trainingData1));

        innerStage = pipe2.getStages()[0];
        innerStage.set(inputColParam, mixedInputCol);
        pipe2.setStages(new PipelineStage[]{innerStage});
        models.add(pipe2.fit(trainingData2));
        return models;
    }

    //step3
    private Dataset joinArtifacts(Dataset d1, Dataset d2) {
        return d1.crossJoin(d2);
    }


    public long runOptimizedSystem() throws Exception {
        long startTime = System.currentTimeMillis();
        code = code.withColumn(CODE_ID, col(codeContent));
        commit = commit.withColumn(COMMIT_ID, col(commitContent));
        bug = bug.withColumn(BUG_ID, col(bugContent));

        List<Dataset> step1 = preprocess(code, commit, bug);
        List<PipelineModel> models = unsupervisedLearn(step1.get(0), step1.get(1), step1.get(2));
        PipelineModel commitIndexModel = models.get(0);
        PipelineModel bugIndexModel = models.get(1);

        String inputColParam = "inputCol";
        String outputColParam = "outputCol";

        commitIndexModel.stages()[0].set(inputColParam, codePreprocess.getOutputField(NGramCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        commitIndexModel.stages()[0].set(outputColParam, codeContentHTF);
        Dataset codeFeatureVec1 = commitIndexModel.transform(step1.get(0)); // code-commit

        bugIndexModel.stages()[0].set(inputColParam, codePreprocess.getOutputField(NGramCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        bugIndexModel.stages()[0].set(outputColParam, codeContentHTF);
        Dataset codeFeatureVec2 = bugIndexModel.transform(step1.get(0)); // code-bug

        commitIndexModel.stages()[0].set(inputColParam, commitPreprocess.getOutputField(NGramCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        commitIndexModel.stages()[0].set(outputColParam, commitContentHTF);
        Dataset commitFeatureVec = commitIndexModel.transform(step1.get(1));

        bugIndexModel.stages()[0].set(inputColParam, bugPreprocess.getOutputField(NGramCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        bugIndexModel.stages()[0].set(outputColParam, bugContentHTF);
        Dataset bugFeatureVec = bugIndexModel.transform(step1.get(2));


        SGraph cosin1 = SparseCosinSimilarityPipeline.getGraph("code-commit-cosin");
        SGraph cosin2 = SparseCosinSimilarityPipeline.getGraph("commit-bug-cosin");
        SGraph cosin3 = SparseCosinSimilarityPipeline.getGraph("code-bug-cosin");

        Dataset codeCommitFeatureVec = joinArtifacts(codeFeatureVec1, commitFeatureVec);
        Dataset commitBugFeatureVec = joinArtifacts(commitFeatureVec, bugFeatureVec);
        Dataset codeBugFeatureVec = joinArtifacts(codeFeatureVec2, bugFeatureVec);

        Map<String, String> codeCommitLinkConfig = new HashMap<>();
        codeCommitLinkConfig.put(SparseCosinSimilarityPipeline.INPUT1, codeContentHTF);
        codeCommitLinkConfig.put(SparseCosinSimilarityPipeline.INPUT2, commitContentHTF);
        Map<String, String> commitBugConfig = new HashMap<>();
        commitBugConfig.put(SparseCosinSimilarityPipeline.INPUT1, commitContentHTF);
        commitBugConfig.put(SparseCosinSimilarityPipeline.INPUT2, bugContentHTF);
        Map<String, String> codeBugConfig = new HashMap<>();
        codeBugConfig.put(SparseCosinSimilarityPipeline.INPUT1, codeContentHTF);
        codeBugConfig.put(SparseCosinSimilarityPipeline.INPUT2, bugContentHTF);

        cosin1.setConfig(codeCommitLinkConfig);
        cosin2.setConfig(commitBugConfig);
        cosin3.setConfig(codeBugConfig);

        Dataset res1 = cosin1.toPipeline().fit(codeCommitFeatureVec).transform(codeCommitFeatureVec);
        Dataset res2 = cosin2.toPipeline().fit(commitBugFeatureVec).transform(commitBugFeatureVec);
        Dataset res3 = cosin3.toPipeline().fit(codeBugFeatureVec).transform(codeBugFeatureVec);

        res1.count();
        res2.count();
        res3.count();
        return System.currentTimeMillis() - startTime;
    }

    public long runUnOptimizedSystem() throws Exception {
        long startTime = System.currentTimeMillis();

        SparkTraceTask job1 = new NGramVSMTraceTask().getTask(CODE_ID, COMMIT_ID);
        job1.setCleanColumns(false);
        job1.indexOn = 1; //index on the target artifacts
        Map<String, String> codeCommitConfig = new HashMap<>();
        codeCommitConfig.put(NGramVSMTraceTask.INPUT1, codeContent);
        codeCommitConfig.put(NGramVSMTraceTask.INPUT2, commitContent);
        job1.setConfig(codeCommitConfig);
        syncSymbolValues(job1);
        job1.train(code, commit, null);
        Dataset<Row> result1 = job1.trace(code, commit);
        result1.count();
        long job1Finished = System.currentTimeMillis();
        long job1Time = job1Finished - startTime;

        SparkTraceTask job2 = new VSMTraceBuilder().getTask(COMMIT_ID, BUG_ID);
        job2.indexOn = 1;
        job2.setCleanColumns(false);
        Map<String, String> commitBugConfig = new HashMap<>();
        commitBugConfig.put(NGramVSMTraceTask.INPUT1, commitContent);
        commitBugConfig.put(NGramVSMTraceTask.INPUT2, bugContent);
        job2.setConfig(commitBugConfig);
        syncSymbolValues(job2);
        job2.train(commit, bug, null);
        Dataset<Row> result2 = job2.trace(commit, bug);
        result2.count();

        SparkTraceTask job3 = new VSMTraceBuilder().getTask(CODE_ID, BUG_ID);
        job2.indexOn = 1;
        job3.setCleanColumns(false);
        Map<String, String> codeBugConfig = new HashMap<>();
        codeBugConfig.put(NGramVSMTraceTask.INPUT1, codeContent);
        codeBugConfig.put(NGramVSMTraceTask.INPUT2, bugContent);
        job3.setConfig(codeBugConfig);
        syncSymbolValues(job3);
        job3.train(code, bug, null);
        Dataset<Row> result3 = job3.trace(code, bug);
        result3.count();

        long job2Time = System.currentTimeMillis() - job1Finished;
        return job1Time + job2Time;
    }

    private void writeResult(Dataset result, String[] cols) {
        String outputFile = RandomStringUtils.randomAlphabetic(5);
        List<Column> columns = (Arrays.asList(cols)).stream().map(name -> col(name)).collect(Collectors.toList());
        result.select(columns.toArray(new Column[0])).write()
                .format("com.databricks.spark.csv")
                .option("header", "true").mode("overwrite")
                .save(outputDir + "/%s.csv".format(outputFile));
    }

    public static void main(String[] args) throws Exception {
        String outputDir = "results"; // "results"
        String codePath = "src/main/resources/maven_sample/code.csv";
        String bugPath = "src/main/resources/maven_sample/bug.csv";
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String sourceCodeRootDir = "src/main/resources/maven_sample";
        Maven3ArtifactsExperiment exp = new Maven3ArtifactsExperiment(codePath, bugPath, commitPath, sourceCodeRootDir);
        long opTime = exp.runOptimizedSystem();
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputDir + "/Maven3ArtifactResultOp.csv");
        OutputStream out = outputPath.getFileSystem(new Configuration()).create(outputPath);
        String opTimeLine = "Op time = " + String.valueOf(opTime) + "\n";
        out.write(opTimeLine.getBytes());
        out.close();
        System.out.println(opTimeLine);
    }
}
