package experiments;

import core.SparkTraceJob;
import core.SparkTraceTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import traceTasks.LinkCompletionTraceTask;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCCLink;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenICLink;
import traceability.components.maven.MavenImprovement;

import java.util.*;

import static core.SparkTraceTask.LabelCol;
import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.*;

/**
 *
 */
public class LCExperiment extends SparkTraceJob {
    private Dataset commits, improvements, improvementCommitLink;
    String outDir;

    public LCExperiment(String commitPath, String improvementPath, String improvementCommitLinkPath, String commitCodeLinkPath, String sparkMod, String outDir) {
        super(sparkMod, "LC  Exp");
        commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        improvementCommitLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementCommitLinkPath, MavenICLink.class).withColumn("label", lit(1));
        Dataset commitCodeLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitCodeLinkPath, MavenCCLink.class);
        commitCodeLink = commitCodeLink.groupBy("commit_id").agg(collect_set(col("class_id")).as("files"));
        Dataset flatImprovementCommitLink = improvementCommitLink.groupBy("issue_id").agg(collect_set(col("commit_id")).as("linked_commit"));
        commits = commits.join(commitCodeLink, scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("commit_id")), "left_outer").cache();
        improvements = improvements.join(flatImprovementCommitLink, scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("issue_id")), "left_outer").cache();
        this.outDir = outDir;
    }

    public long runExperiment() throws Exception {
        SparkTraceTask task = new LinkCompletionTraceTask().getTask("commit_id", "issue_id");
        Map<String, String> config = new HashMap<>();
        config.put(LinkCompletionTraceTask.COMMIT_ID, "commit_id");
        config.put(LinkCompletionTraceTask.S_TEXT, "commit_content");
        config.put(LinkCompletionTraceTask.T_TEXT, "issue_content");
        config.put(LinkCompletionTraceTask.COMMIT_TIME, "commit_date");
        config.put(LinkCompletionTraceTask.COMMIT_AUTHOR, "commit_author");
        config.put(LinkCompletionTraceTask.FILES, "files");
        config.put(LinkCompletionTraceTask.LINKED_COMMIT, "linked_commit");
        config.put(LinkCompletionTraceTask.ISSUE_RESOLVE, "issue_resolved_date");
        config.put(LinkCompletionTraceTask.ISSUE_CREATE, "issue_created_date");
        config.put(LinkCompletionTraceTask.TRAIN_LABEL, LabelCol);
        task.setConfig(config);
        //task.getDdfGraph().optimize(task.getDdfGraph()); //optimized: 1m28ms unoptimized: 1m45ms including startup time
        syncSymbolValues(task);
        long startTime = System.currentTimeMillis();
        task.train(commits, improvements, improvementCommitLink);
        Dataset result = task.trace(commits, improvements).select("commit_id", "issue_id", "probability").withColumn("probability", col("probability").cast(DataTypes.StringType));
        result.count();
        //result.write().csv(outDir + "/result.csv");
        return System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) throws Exception {
        String mavenDir = args[0]; //"src/main/resources/git_projects"
        String sparkMod = args[1];
        String outDir = args[2];
        String commitPath = mavenDir + "/commits.csv";
        String improvementPath = mavenDir + "/improvement.csv";
        String improvementCommitLinkPath = mavenDir + "improvementCommitLinks.csv";
        String commitCodeLinkPath = mavenDir + "/CommitCodeLinks.csv";
        long average = 0;
        for (int i = 0; i < 1; i++) {
            LCExperiment lc = new LCExperiment(commitPath, improvementPath, improvementCommitLinkPath, commitCodeLinkPath, sparkMod, outDir);
            long time = lc.runExperiment();
            average += time;
            System.out.println(time);
        }
        System.out.println(average);
    }
}
