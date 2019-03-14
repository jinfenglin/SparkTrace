import examples.TestBase;
import featurePipelineStages.cloestLinkedCommit.CLTimeDiff;
import featurePipelineStages.cloestLinkedCommit.CLUser;
import featurePipelineStages.cloestLinkedCommit.FindClosestPreviousLinkedCommit;
import featurePipelineStages.cloestLinkedCommit.Overlap;
import org.apache.spark.sql.Dataset;
import org.junit.Test;
import traceability.TraceDatasetFactory;
import traceability.components.maven.MavenCCLink;
import traceability.components.maven.MavenCommit;
import traceability.components.maven.MavenICLink;
import traceability.components.maven.MavenImprovement;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;

/**
 *
 */
public class ICSEExample extends TestBase {
    private static String masterUrl = "local";
    private Dataset dataset;

    public ICSEExample() {
        super(masterUrl);
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String improvementPath = "src/main/resources/maven_sample/improvement.csv";
        String improvementCommitLinkPath = "src/main/resources/maven_sample/improvementCommitLinks.csv";
        String commitCodeLinkPath = "src/main/resources/maven_sample/CommitCodeLinks.csv";
        Dataset commits = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitPath, MavenCommit.class);
        Dataset improvements = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementPath, MavenImprovement.class);
        Dataset improvementCommitLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, improvementCommitLinkPath, MavenICLink.class);
        Dataset commitCodeLink = TraceDatasetFactory.createDatasetFromCSV(sparkSession, commitCodeLinkPath, MavenCCLink.class);
        commitCodeLink = commitCodeLink.groupBy("commit_id").agg(collect_set(col("class_id")).as("files"));
        improvementCommitLink = improvementCommitLink.groupBy("issue_id").agg(collect_set(col("commit_id")).as("linked_commit"));
        commits = commits.join(commitCodeLink, "commit_id");
        improvements = improvements.join(improvementCommitLink, "issue_id");
        dataset = commits.crossJoin(improvements);
    }

    @Test
    public void findClosestLinkedCommit() {
        FindClosestPreviousLinkedCommit fcpl = new FindClosestPreviousLinkedCommit();
        fcpl.set(fcpl.inputCols(), new String[]{"commit_id", "commit_date", "linked_commit"});
        fcpl.set("isPreviousClosest", true);
        fcpl.set(fcpl.outputCol(), "closestPreviousCommit");
        fcpl.transform(dataset).show(false);
    }

    @Test
    public void findOverlap() {
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


}
