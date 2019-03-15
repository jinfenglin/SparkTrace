package traceTasks;


import buildingBlocks.ICSEFeatures.LCA4ToA7;
import buildingBlocks.preprocessor.SimpleWordCount;
import buildingBlocks.randomForestPipeline.RandomForestPipeline;
import buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;
import org.apache.spark.sql.Dataset;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 *
 */
public class LinkCompletionTraceTask {
    public static String S_TEXT = "S_TEXT", T_TEXT = "T_TEXT", TRAIN_LABEL = "TRAIN_LABEL";
    public static String COMMIT_TIME = "COMMIT_TIME", ISSUE_CREATE = "ISSUE_CREATE", ISSUE_RESOLVE = "ISSUE_RESOLVE";//inputCol Symbols
    public static String PREDICTION = "PREDICTION";
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

    public SGraph createModelGraph() throws Exception {
        return RandomForestPipeline.getGraph("RandomForest", new String[]{A4, A5, A6, A7, DOC_SIM});
    }

    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        SGraph SSDF = task.getSourceSDFSdfGraph();
        SGraph TSDF = task.getTargetSDFSdfGraph();
        SGraph DDF = task.getDdfGraph();
        SGraph model = task.getPredictGraph();
        SGraph unsuperviseGraph = task.getUnsupervisedLearnGraph().get(0);
        task.connect(task.sourceNode, S_TEXT, SSDF, SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.sourceNode, T_TEXT, TSDF, SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.sourceNode, COMMIT_TIME, DDF, COMMIT_TIME);
        task.connect(task.sourceNode, ISSUE_CREATE, DDF, ISSUE_CREATE);
        task.connect(task.sourceNode, ISSUE_RESOLVE, DDF, ISSUE_RESOLVE);
        task.connect(SSDF, SimpleWordCount.OUTPUT_HTF, unsuperviseGraph, IDFGraphPipeline.INPUT1);
        task.connect(TSDF, SimpleWordCount.OUTPUT_HTF, unsuperviseGraph, IDFGraphPipeline.INPUT2);
        task.connect(unsuperviseGraph, IDFGraphPipeline.OUTPUT1, DDF, SHTF);
        task.connect(unsuperviseGraph, IDFGraphPipeline.OUTPUT2, DDF, THTF);

        task.connect(DDF, LCA4ToA7.A4, model, A4);
        task.connect(DDF, LCA4ToA7.A5, model, A5);
        task.connect(DDF, LCA4ToA7.A6, model, A6);
        task.connect(DDF, LCA4ToA7.A7, model, A7);
        task.connect(DDF, DOC_SIM, model, DOC_SIM);
        task.connect(task.sourceNode, TRAIN_LABEL, model, TRAIN_LABEL);
        task.connect(model, PREDICTION, task.sinkNode, PREDICTION);
        return task;
    }

    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSSDF(), createTSDF(), Arrays.asList(createUnsupervise()), createDDF(), sourceId, targetId);
        task.setPredictGraph(createModelGraph());
        task.setVertexLabel("ICSE LC");
        task.addInputField(S_TEXT).addInputField(T_TEXT).addInputField(TRAIN_LABEL);
        task.addInputField(COMMIT_TIME);
        task.addInputField(ISSUE_RESOLVE);
        task.addInputField(ISSUE_CREATE);
        task.addOutputField(PREDICTION);
        connectTask(task);
        return task;
    }
}
