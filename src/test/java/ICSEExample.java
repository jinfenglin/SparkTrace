import core.SparkTraceTask;
import examples.TestBase;
import featurePipelineStages.cloestLinkedCommit.CLTimeDiff;
import featurePipelineStages.cloestLinkedCommit.CLUser;
import featurePipelineStages.cloestLinkedCommit.FindClosestPreviousLinkedCommit;
import featurePipelineStages.cloestLinkedCommit.Overlap;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import traceTasks.LinkCompletionTraceTask;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCCLink;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenICLink;
import traceability.components.maven.MavenImprovement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static core.SparkTraceTask.LabelCol;
import static core.graphPipeline.basic.SGraph.syncSymbolValues;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 *
 */
public class ICSEExample extends TestBase {
    private static String masterUrl = "local";
    private Dataset commits, improvements, improvementCommitLink;

    public ICSEExample() {
        super(masterUrl);
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        String improvementCommitLinkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        String commitCodeLinkPath = "src/main/resources/maven_sample/CommitCodeLinks.csv";
        commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        improvementCommitLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementCommitLinkPath, MavenICLink.class).withColumn("label", lit(1));
        Dataset commitCodeLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitCodeLinkPath, MavenCCLink.class);
        commitCodeLink = commitCodeLink.groupBy("commit_id").agg(collect_set(col("class_id")).as("files"));
        Dataset flatImprovementCommitLink = improvementCommitLink.groupBy("issue_id").agg(collect_set(col("commit_id")).as("linked_commit"));
        commits = commits.join(commitCodeLink, scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("commit_id")), "left_outer");
        improvements = improvements.join(flatImprovementCommitLink, scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("issue_id")), "left_outer");
    }

    @Test
    public void findClosestLinkedCommit() {
        Dataset dataset = commits.crossJoin(improvements);
        FindClosestPreviousLinkedCommit fcpl = new FindClosestPreviousLinkedCommit();
        fcpl.set(fcpl.inputCols(), new String[]{"commit_id", "commit_date", "linked_commit"});
        fcpl.set("isPreviousClosest", true);
        fcpl.set(fcpl.outputCol(), "closestPreviousCommit");
        fcpl.transform(dataset).show(false);
    }

    @Test
    public void findOverlap() {
        Dataset dataset = commits.crossJoin(improvements);
        FindClosestPreviousLinkedCommit fcpl = new FindClosestPreviousLinkedCommit();
        fcpl.set(fcpl.inputCols(), new String[]{"commit_id", "commit_date", "linked_commit"});
        fcpl.set("isPreviousClosest", true);
        fcpl.set(fcpl.outputCol(), "closestPreviousCommit");
        dataset = fcpl.transform(dataset);
        Overlap overlap = new Overlap();
        overlap.set(overlap.inputCols(), new String[]{"commit_id", "files", "closestPreviousCommit"});
        overlap.set(overlap.outputCol(), "overlap_percent");
        overlap.transform(dataset).orderBy(col("overlap_percent").desc()).show();
    }

    @Test
    public void findCLTimeDiff() {
        Dataset dataset = dataset = commits.crossJoin(improvements);
        FindClosestPreviousLinkedCommit fcpl = new FindClosestPreviousLinkedCommit();
        fcpl.set(fcpl.inputCols(), new String[]{"commit_id", "commit_date", "linked_commit"});
        fcpl.set("isPreviousClosest", true);
        fcpl.set(fcpl.outputCol(), "closestPreviousCommit");
        dataset = fcpl.transform(dataset);

        CLTimeDiff clTimeDiff = new CLTimeDiff();
        clTimeDiff.set(clTimeDiff.inputCols(), new String[]{"commit_id", "commit_date", "closestPreviousCommit"});
        clTimeDiff.set(clTimeDiff.outputCol(), "clTimeDiff");
        clTimeDiff.transform(dataset).show();
    }

    @Test
    public void findCLUser() {
        Dataset dataset = commits.crossJoin(improvements);
        FindClosestPreviousLinkedCommit fcpl = new FindClosestPreviousLinkedCommit();
        fcpl.set(fcpl.inputCols(), new String[]{"commit_id", "commit_date", "linked_commit"});
        fcpl.set("isPreviousClosest", true);
        fcpl.set(fcpl.outputCol(), "closestPreviousCommit");
        dataset = fcpl.transform(dataset);

        CLUser clUser = new CLUser();
        clUser.set(clUser.inputCols(), new String[]{"commit_id", "commit_author", "closestPreviousCommit"});
        clUser.set(clUser.outputCol(), "clUser");
        clUser.transform(dataset).show();
    }

    @Test
    public void taskTest() throws Exception {
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
        syncSymbolValues(task);
        task.train(commits, improvements, improvementCommitLink);
        task.trace(commits, improvements).show();
    }
}
