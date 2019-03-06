package experiments;

import buildingBlocks.preprocessor.SimpleWordCount;
import buildingBlocks.traceTasks.VSMTraceBuilder;
import buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceJob;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.col;

/**
 *
 */
public class Maven3ArtifactsExperiment extends SparkTraceJob {
    Dataset<Row> code, bug, commit;
    String CODE_ID = "code_id", BUG_ID = "bug_id", COMMIT_ID = "commit_id";
    String codeContent = "code_content", commitContent = "commit_content", bugContent = "bug_content";
    String codeContentHTF = "code_htf", commitContentHTF1 = "commit_htf1", commitContentHTF2 = "commit_htf2", bugContentHTF = "bug_htf";

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
        bugPreprocess = SimpleWordCount.getGraph("commit_preprocess");
        codePreprocess = SimpleWordCount.getGraph("code_preprocess");
        commitPreprocess = SimpleWordCount.getGraph("commit_process");

        Map<String, String> codeConfig = new HashMap<>();
        Map<String, String> bugConfig = new HashMap<>();
        Map<String, String> commitConfig = new HashMap<>();

        codeConfig.put(SimpleWordCount.INPUT_TEXT_COL, codeContent);
        commitConfig.put(SimpleWordCount.INPUT_TEXT_COL, commitContent);
        bugConfig.put(SimpleWordCount.INPUT_TEXT_COL, bugContent);


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
        String mixedInputCol = "tmpMixedCol";
        StructField field = DataTypes.createStructField(mixedInputCol, new VectorUDT(), false);
        StructType st = new StructType(new StructField[]{field});
        Dataset<Row> trainingData1 = code.sparkSession().createDataFrame(new ArrayList<>(), st);
        Dataset<Row> trainingData2 = code.sparkSession().createDataFrame(new ArrayList<>(), st);

        Dataset<Row> codeHtf = code.select(codePreprocess.getOutputField(SimpleWordCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        Dataset<Row> commitHtf = commit.select(commitPreprocess.getOutputField(SimpleWordCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        Dataset<Row> bugHtf = bug.select(bugPreprocess.getOutputField(SimpleWordCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());

        trainingData1 = trainingData1.union(codeHtf);
        trainingData1 = trainingData1.union(commitHtf);
        trainingData2 = trainingData2.union(commitHtf);
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
        PipelineModel codeCommitModel = models.get(0);
        PipelineModel commitBugModel = models.get(1);
        String inputColParam = "inputCol";
        String outputColParam = "outputCol";

        codeCommitModel.stages()[0].set(inputColParam, codePreprocess.getOutputField(SimpleWordCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        codeCommitModel.stages()[0].set(outputColParam, codeContentHTF);
        Dataset codeFeatureVec = codeCommitModel.transform(step1.get(0)); //code-commit-bug

        codeCommitModel.stages()[0].set(inputColParam, commitPreprocess.getOutputField(SimpleWordCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        codeCommitModel.stages()[0].set(outputColParam, commitContentHTF1);
        Dataset commitFeatureVec1 = codeCommitModel.transform(step1.get(1));

        commitBugModel.stages()[0].set(inputColParam, commitPreprocess.getOutputField(SimpleWordCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        commitBugModel.stages()[0].set(outputColParam, commitContentHTF2);
        Dataset commitFeatureVec2 = commitBugModel.transform(step1.get(1));

        commitBugModel.stages()[0].set(inputColParam, bugPreprocess.getOutputField(SimpleWordCount.OUTPUT_HTF).getFieldSymbol().getSymbolValue());
        commitBugModel.stages()[0].set(outputColParam, bugContentHTF);
        Dataset bugFeatureVec = commitBugModel.transform(step1.get(2));


        Dataset codeCommitFeatureVec = joinArtifacts(codeFeatureVec, commitFeatureVec1);
        Dataset commitBugFeatureVec = joinArtifacts(commitFeatureVec2, bugFeatureVec);

        SGraph cosin1 = SparseCosinSimilarityPipeline.getGraph("code-commit-cosin");
        SGraph cosin2 = SparseCosinSimilarityPipeline.getGraph("commit-bug-cosin");

        Map<String, String> codeCommitLinkConfig = new HashMap<>();
        codeCommitLinkConfig.put(SparseCosinSimilarityPipeline.INPUT1, codeContentHTF);
        codeCommitLinkConfig.put(SparseCosinSimilarityPipeline.INPUT2, commitContentHTF1);
        Map<String, String> commitBugConfig = new HashMap<>();
        commitBugConfig.put(SparseCosinSimilarityPipeline.INPUT1, commitContentHTF2);
        commitBugConfig.put(SparseCosinSimilarityPipeline.INPUT2, bugContentHTF);
        cosin1.setConfig(codeCommitLinkConfig);
        Dataset res1 = cosin1.toPipeline().fit(codeCommitFeatureVec).transform(codeCommitFeatureVec);

        cosin2.setConfig(commitBugConfig);
        Dataset res2 = cosin2.toPipeline().fit(commitBugFeatureVec).transform(commitBugFeatureVec);
        res1.count();
        res2.count();
        return System.currentTimeMillis() - startTime;
    }

    public long runUnOptimizedSystem() throws Exception {
        long startTime = System.currentTimeMillis();

        SparkTraceTask job1 = new VSMTraceBuilder().getTask(CODE_ID, COMMIT_ID);
        job1.setCleanColumns(false);
        Map<String, String> codeCommitConfig = new HashMap<>();
        codeCommitConfig.put(VSMTraceBuilder.INPUT_TEXT1, codeContent);
        codeCommitConfig.put(VSMTraceBuilder.INPUT_TEXT2, commitContent);
        job1.setConfig(codeCommitConfig);
        syncSymbolValues(job1);
        job1.train(code, commit, null);
        Dataset<Row> result1 = job1.trace(code, commit);
        result1.count();

        SparkTraceTask job2 = new VSMTraceBuilder().getTask(COMMIT_ID, BUG_ID);
        job2.setCleanColumns(false);
        Map<String, String> commitBugConfig = new HashMap<>();
        commitBugConfig.put(VSMTraceBuilder.INPUT_TEXT1, commitContent);
        commitBugConfig.put(VSMTraceBuilder.INPUT_TEXT2, bugContent);
        job2.setConfig(commitBugConfig);
        syncSymbolValues(job2);
        job2.train(commit, bug, null);
        Dataset<Row> result2 = job2.trace(commit, bug);
        result2.count();

        return System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "G:\\tools\\spark-2.4.0-bin-hadoop2.7");
        String outputDir = "results"; // "results"

        String codePath = "src/main/resources/maven_sample/code.csv";
        String bugPath = "src/main/resources/maven_sample/bug.csv";
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String sourceCodeRootDir = "src/main/resources/maven_sample";
        Maven3ArtifactsExperiment exp = new Maven3ArtifactsExperiment(codePath, bugPath, commitPath, sourceCodeRootDir);
        long opTime = exp.runOptimizedSystem();
        long unOpTime = exp.runUnOptimizedSystem();

        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputDir + "/Maven3ArtifactResult.csv");
        OutputStream out = outputPath.getFileSystem(new Configuration()).create(outputPath);
        String opTimeLine = "Op time = " + String.valueOf(opTime) + "\n";
        String unOpTimeLine = "Unop Time = " + String.valueOf(unOpTime) + "\n";
        out.write(opTimeLine.getBytes());
        out.write(unOpTimeLine.getBytes());
        out.close();
        System.out.println(opTime);
        System.out.println(unOpTime);
    }
}
