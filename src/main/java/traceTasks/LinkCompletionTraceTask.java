package traceTasks;


import buildingBlocks.ICSEFeatures.LCA4ToA7;
import buildingBlocks.preprocessor.NGramCount;
import buildingBlocks.preprocessor.SimpleWordCount;
import buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCCLink;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenImprovement;
import traceability.components.maven.MavenICLink;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 *
 */
public class LinkCompletionTraceTask {
    public static String
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
        SGraph cosinSubGraph = SparseCosinSimilarityPipeline.getGraph("Cosin"); //vec1,2 - cosin_sim
        SGraph lca4To7 = LCA4ToA7.getGraph("lca4To7");

        ddfGraph.addNode(cosinSubGraph);
        ddfGraph.addNode(lca4To7);
        

        return ddfGraph;
    }

    public SGraph createUnsupervise() throws Exception {
        return IDFGraphPipeline.getGraph("SharedIDF");
    }

    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
    }

    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSSDF(), createTSDF(), Arrays.asList(createUnsupervise()), createDDF(), sourceId, targetId);
        task.setVertexLabel("ICSE LC");
        connectTask(task);
        return task;
    }

    public static void main(String[] args) {
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

    }
}
