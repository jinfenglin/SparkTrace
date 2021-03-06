package experiments;

import componentRepo.SLayer.buildingBlocks.preprocessor.NGramCount;
import traceTasks.NGramVSMTraceTaskBuilder;
import traceTasks.VSMTraceBuilder;
import componentRepo.SLayer.buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import componentRepo.SLayer.buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceJob;
import core.SparkTraceTask;
import core.graphPipeline.SLayer.SGraph;
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

import static core.graphPipeline.SLayer.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.col;

/**
 *
 */
public class Maven3ArtifactsExperiment extends SparkTraceJob {
    Dataset<Row> code, bug, commit;
    String CODE_ID = "code_id", BUG_ID = "bug_id", COMMIT_ID = "commit_id";
    String codeContent = "code_content", commitContent = "commit_content", bugContent = "issue_content";
    String codeContentHTF = "code_htf", commitContentHTF = "commit_htf", bugContentHTF = "bug_htf";
    String outputDir = "tmp";
    SGraph bugPreprocess, codePreprocess, commitPreprocess;

    public Maven3ArtifactsExperiment(String codeFilePath, String bugPath, String commitPath, String sourceCodeRootDir) throws IOException {
        super("local[*]", "Maven Cross Trace");
        bug = sparkSession.read().option("header", "true").csv(bugPath);
        bug = bug.where(col(bugContent).isNotNull());
        commit = sparkSession.read().option("header", "true").csv(commitPath);
        code = readCodeFromFile(codeFilePath, sourceCodeRootDir);

        code = code.withColumn(CODE_ID, col(codeContent)).cache();
        commit = commit.withColumn(COMMIT_ID, col(commitContent)).cache();
        bug = bug.withColumn(BUG_ID, col(bugContent)).cache();
    }

    private Map<String, String> getConfig() {
        Map<String, String> config = new HashMap<>();
        return config;
    }

    private Dataset<Row> readCodeFromFile(String codeFilePath, String codeDirPath) throws IOException {
        File codeIdFile = new File(codeFilePath);
        List<String> lines = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(codeIdFile));
        String line = null;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        //List<String> lines = Files.readAllLines(codeIdFile.toPath());
        lines.remove(0);
        List<Row> rows = new ArrayList<>();
        for (String codeId : lines) {
            try {
                Path codeFile = Paths.get(codeDirPath, codeId.split(",")[0]);
                String content = new String(Files.readAllBytes(codeFile));
                rows.add(RowFactory.create(codeId, content));
            } catch (Exception e) {
                System.out.println(String.format("Skip %s for error", codeId));
            }
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
        List<Dataset> step1 = preprocess(code, commit, bug);
        long train_start = System.currentTimeMillis();
        List<PipelineModel> models = unsupervisedLearn(step1.get(0), step1.get(1), step1.get(2));
        long train_time = System.currentTimeMillis() - train_start;

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

        System.out.println(res1.count());
        System.out.println(res2.count());
        System.out.println(res3.count());
        return System.currentTimeMillis() - startTime - train_time;
    }

    public long runUnOptimizedSystem() throws Exception {
        long startTime = System.currentTimeMillis();
        SparkTraceTask job1 = new NGramVSMTraceTaskBuilder().getTask(CODE_ID, COMMIT_ID);
        job1.indexOn = 1; //index on the target artifacts
        Map<String, String> codeCommitConfig = new HashMap<>();
        codeCommitConfig.put(NGramVSMTraceTaskBuilder.INPUT1, codeContent);
        codeCommitConfig.put(NGramVSMTraceTaskBuilder.INPUT2, commitContent);
        job1.setConfig(codeCommitConfig);
        syncSymbolValues(job1);
        job1.train(code, commit, null);
        Dataset<Row> result1 = job1.trace(code, commit);
        System.out.println(result1.count());

        SparkTraceTask job2 = new VSMTraceBuilder().getTask(COMMIT_ID, BUG_ID);
        job2.indexOn = 1;
        Map<String, String> commitBugConfig = new HashMap<>();
        commitBugConfig.put(NGramVSMTraceTaskBuilder.INPUT1, commitContent);
        commitBugConfig.put(NGramVSMTraceTaskBuilder.INPUT2, bugContent);
        job2.setConfig(commitBugConfig);
        syncSymbolValues(job2);
        job2.train(commit, bug, null);
        Dataset<Row> result2 = job2.trace(commit, bug);
        System.out.println(result2.count());

        SparkTraceTask job3 = new VSMTraceBuilder().getTask(CODE_ID, BUG_ID);
        job3.indexOn = 1;
        Map<String, String> codeBugConfig = new HashMap<>();
        codeBugConfig.put(NGramVSMTraceTaskBuilder.INPUT1, codeContent);
        codeBugConfig.put(NGramVSMTraceTaskBuilder.INPUT2, bugContent);
        job3.setConfig(codeBugConfig);
        syncSymbolValues(job3);
        job3.train(code, bug, null);
        Dataset<Row> result3 = job3.trace(code, bug);
        System.out.println(result3.count());

        return System.currentTimeMillis() - startTime;
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
        String dataDirRoot = "G://Document//data_csv";
        List<String> projects = new ArrayList<>();
        projects.addAll(Arrays.asList(new String[]{"derby", "drools", "groovy", "infinispan", "maven", "pig", "seam2"}));
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputDir + "/Maven3ArtifactResultOp.csv");
        OutputStream out = outputPath.getFileSystem(new Configuration()).create(outputPath);
        for (String projectPath : projects) {
            String codePath = Paths.get(dataDirRoot, projectPath, "code.csv").toString();
            String bugPath = Paths.get(dataDirRoot, projectPath, "bug.csv").toString();
            String commitPath = Paths.get(dataDirRoot, projectPath, "commits.csv").toString();
            String sourceCodeRootDir = Paths.get(dataDirRoot, projectPath).toString();
            System.out.println(projectPath);
            Maven3ArtifactsExperiment exp = new Maven3ArtifactsExperiment(codePath, bugPath, commitPath, sourceCodeRootDir);
            long opTime = exp.runOptimizedSystem();
            String opTimeLine = "Op time = " + String.valueOf(opTime) + "\n";
            out.write(opTimeLine.getBytes());
            System.out.println(opTimeLine);
        }
        out.close();
    }
}
