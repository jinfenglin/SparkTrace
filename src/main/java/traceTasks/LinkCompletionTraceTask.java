package traceTasks;


import buildingBlocks.ICSEFeatures.LCA4ToA7;
import buildingBlocks.preprocessor.NGramCount;
import buildingBlocks.preprocessor.SimpleWordCount;
import buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCCLink;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenICLink;

import java.util.Arrays;

import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.*;

/**
 *
 */
public class LinkCompletionTraceTask {
    public static String S_TEXT = "S_TEXT", T_TEXT = "T_TEXT";
    public static String COMMIT_TIME = "COMMIT_TIME", ISSUE_CREATE = "ISSUE_CREATE", ISSUE_RESOLVE = "ISSUE_RESOLVE";//inputCol Symbols
    public static String SHTF = "SHTF", THTF = "THTF";//DDF input output
    public static String DOC_SIM = "DOC_SIM";
    public static String A4 = "A4", A5 = "A5", A6 = "A6", A7 = "A7";

    public LinkCompletionTraceTask() {

    }

    public void train(Dataset commits, Dataset issues, Dataset issueCommitLink, Dataset commitCodeLink) {
        //flatten(commitIssueLink);
        commitCodeLink = commitCodeLink.groupBy("commit_id").agg(collect_set(col("class_id")).as("files"));
        issueCommitLink = issueCommitLink.groupBy("issue_id").agg(collect_set(col("commit_id")).as("linked_commit"));
        commits = commits.join(commitCodeLink, "commit_id");
        issues = issues.join(issueCommitLink, "issue_id");
    }

    public SGraph createSSDF() throws Exception {
        SGraph graph = SimpleWordCount.getGraph("VSM_SSDF");
        return graph;
    }

    public SGraph createTSDF() throws Exception {
        SGraph graph = SimpleWordCount.getGraph("VSM_TSDF");
        return graph;
    }

    public SGraph createDDF() throws Exception {
        SGraph ddfGraph = new SGraph("LC_DDF");
        ddfGraph.addInputField(SHTF);
        ddfGraph.addInputField(THTF);
        ddfGraph.addInputField(COMMIT_TIME);
        ddfGraph.addInputField(ISSUE_CREATE);
        ddfGraph.addInputField(ISSUE_RESOLVE);

        ddfGraph.addOutputField(A4);
        ddfGraph.addOutputField(A5);
        ddfGraph.addOutputField(A6);
        ddfGraph.addOutputField(A7);
        ddfGraph.addOutputField(DOC_SIM);

        SGraph cosinSubGraph = SparseCosinSimilarityPipeline.getGraph("Cosin"); //vec1,2 - cosin_sim
        SGraph lca4To7 = LCA4ToA7.getGraph("lca4To7");

        ddfGraph.addNode(cosinSubGraph);
        ddfGraph.addNode(lca4To7);

        ddfGraph.connect(ddfGraph.sourceNode, SHTF, cosinSubGraph, SparseCosinSimilarityPipeline.INPUT1);
        ddfGraph.connect(ddfGraph.sourceNode, THTF, cosinSubGraph, SparseCosinSimilarityPipeline.INPUT2);
        ddfGraph.connect(ddfGraph.sourceNode, COMMIT_TIME, lca4To7, LCA4ToA7.COMMIT_TIME);
        ddfGraph.connect(ddfGraph.sourceNode, ISSUE_CREATE, lca4To7, LCA4ToA7.ISSUE_CREATE);
        ddfGraph.connect(ddfGraph.sourceNode, ISSUE_RESOLVE, lca4To7, LCA4ToA7.ISSUE_RESOLVE);
        ddfGraph.connect(cosinSubGraph, SparseCosinSimilarityPipeline.OUTPUT, ddfGraph.sinkNode, DOC_SIM);
        ddfGraph.connect(lca4To7, LCA4ToA7.A4, ddfGraph.sinkNode, A4);
        ddfGraph.connect(lca4To7, LCA4ToA7.A5, ddfGraph.sinkNode, A5);
        ddfGraph.connect(lca4To7, LCA4ToA7.A6, ddfGraph.sinkNode, A6);
        ddfGraph.connect(lca4To7, LCA4ToA7.A7, ddfGraph.sinkNode, A7);
        return ddfGraph;
    }

    public SGraph createUnsupervise() throws Exception {
        return IDFGraphPipeline.getGraph("SharedIDF");
    }

    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        SGraph SSDF = task.getSourceSDFSdfGraph();
        SGraph TSDF = task.getTargetSDFSdfGraph();
        SGraph DDF = task.getDdfGraph();
        SGraph unsuperviseGraph = task.getUnsupervisedLearnGraph().get(0);
        task.connect(task.sourceNode, S_TEXT, SSDF, SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.sourceNode, T_TEXT, TSDF, SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.sourceNode, COMMIT_TIME, DDF, COMMIT_TIME);
        task.connect(task.sourceNode, ISSUE_CREATE, DDF, ISSUE_CREATE);
        task.connect(task.sourceNode, ISSUE_RESOLVE, DDF, ISSUE_RESOLVE);
        task.connect(SSDF, SimpleWordCount.OUTPUT_HTF, unsuperviseGraph,  IDFGraphPipeline.INPUT1);
        task.connect(TSDF, SimpleWordCount.OUTPUT_HTF, unsuperviseGraph,  IDFGraphPipeline.INPUT2);
        task.connect(unsuperviseGraph, IDFGraphPipeline.OUTPUT1, DDF, SHTF);
        task.connect(unsuperviseGraph, IDFGraphPipeline.OUTPUT2, DDF, THTF);

        task.connect(DDF, LCA4ToA7.A4, task.sinkNode, A4);
        task.connect(DDF, LCA4ToA7.A5, task.sinkNode, A5);
        task.connect(DDF, LCA4ToA7.A6, task.sinkNode, A6);
        task.connect(DDF, LCA4ToA7.A7, task.sinkNode, A7);
        task.connect(DDF, DOC_SIM, task.sinkNode, DOC_SIM);
        return task;
    }

    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSSDF(), createTSDF(), Arrays.asList(createUnsupervise()), createDDF(), sourceId, targetId);
        task.setVertexLabel("ICSE LC");
        task.addInputField(S_TEXT).addInputField(T_TEXT);
        task.addInputField(COMMIT_TIME);
        task.addInputField(ISSUE_RESOLVE);
        task.addInputField(ISSUE_CREATE);
        task.addOutputField(A4);
        task.addOutputField(A5);
        task.addOutputField(A6);
        task.addOutputField(A7);
        task.addOutputField(DOC_SIM);
        connectTask(task);
        return task;
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[4]");
        conf.setAppName("playground");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        String improvementCommitLinkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        String commitCodeLinkPath = "src/main/resources/maven_sample/CommitCodeLinks.csv";
        Dataset commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        Dataset improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        Dataset improvementCommitLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementCommitLinkPath, MavenICLink.class);
        Dataset commitCodeLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitCodeLinkPath, MavenCCLink.class);

        SparkTraceTask task = new LinkCompletionTraceTask().getTask("commmit_id", "issue_id");
        syncSymbolValues(task);
        task.train(commits, improvements, null);
        task.trace(commits, improvements).show();
    }
}
