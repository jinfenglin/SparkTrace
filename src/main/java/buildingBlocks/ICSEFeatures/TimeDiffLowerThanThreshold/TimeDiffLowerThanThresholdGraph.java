package buildingBlocks.ICSEFeatures.TimeDiffLowerThanThreshold;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import featurePipelineStages.temporalRelations.CompareThreshold;
import featurePipelineStages.temporalRelations.TimeDiff;

/**
 *
 */
public class TimeDiffLowerThanThresholdGraph {
    public static String COMMIT_TIME = "COMMIT_TIME", ISSUE_RESOLVE = "ISSUE_RESOLVE";
    public static String TIME_DIFF = "TIME_DIFF", OUTPUT = "OUTPUT";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);

        graph.addInputField(COMMIT_TIME);
        graph.addInputField(ISSUE_RESOLVE);
        graph.addOutputField(OUTPUT);

        SNode tillIssueResolveNode = new SNode(new TimeDiff(), "TillIssueResolve");
        tillIssueResolveNode.addInputField(ISSUE_RESOLVE);
        tillIssueResolveNode.addInputField(COMMIT_TIME);
        tillIssueResolveNode.addOutputField(TIME_DIFF);

        CompareThreshold ct = new CompareThreshold();
        ct.set("threshold", 2.5);
        SNode lowerThanThreshold = new SNode(ct, "lowerThanThreshold");
        lowerThanThreshold.addInputField(TIME_DIFF);
        lowerThanThreshold.addOutputField(OUTPUT);

        graph.addNode(tillIssueResolveNode);
        graph.addNode(lowerThanThreshold);

        graph.connect(graph.sourceNode, COMMIT_TIME, tillIssueResolveNode, COMMIT_TIME);
        graph.connect(graph.sourceNode, ISSUE_RESOLVE, tillIssueResolveNode, ISSUE_RESOLVE);
        graph.connect(tillIssueResolveNode, TIME_DIFF, lowerThanThreshold, TIME_DIFF);
        graph.connect(lowerThanThreshold, OUTPUT, graph.sinkNode, OUTPUT);
        return graph;
    }
}
