package buildingBlocks.ICSEFeatures;

import buildingBlocks.ICSEFeatures.TimeDiffLowerThanThreshold.TimeDiffLowerThanThresholdGraph;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.temporalRelations.InTimeRange;
import featurePipelineStages.temporalRelations.TimeDiff;

public class LCA4ToA7 {
    public static String COMMIT_TIME = "COMMIT_TIME", ISSUE_CREATE = "ISSUE_CREATE", ISSUE_RESOLVE = "ISSUE_RESOLVE";
    public static String A4 = "A4", A5 = "A5", A6 = "A6", A7 = "A7";

    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField(COMMIT_TIME);
        graph.addInputField(ISSUE_CREATE);
        graph.addInputField(ISSUE_RESOLVE);
        graph.addOutputField(A5);
        graph.addOutputField(A6);
        graph.addOutputField(A7);


        SNode tillIssueCreateNode = new SNode(new TimeDiff(), "TillIssueCreation");
        tillIssueCreateNode.addInputField(COMMIT_TIME);
        tillIssueCreateNode.addInputField(ISSUE_CREATE);
        tillIssueCreateNode.addOutputField(A4);

        SNode tillIssueResolveNode = new SNode(new TimeDiff(), "TillIssueResolve");
        tillIssueResolveNode.addInputField(ISSUE_RESOLVE);
        tillIssueResolveNode.addInputField(COMMIT_TIME);
        tillIssueResolveNode.addOutputField(A5);

        SNode inRange = new SNode(new InTimeRange(), "InTimeRange");
        inRange.addInputField(ISSUE_CREATE);
        inRange.addInputField(COMMIT_TIME);
        inRange.addInputField(ISSUE_RESOLVE);
        inRange.addOutputField(A6);

        SGraph A7Graph = TimeDiffLowerThanThresholdGraph.getGraph("A7Graph");
        graph.addNode(tillIssueCreateNode);
        graph.addNode(tillIssueResolveNode);
        graph.addNode(inRange);
        graph.addNode(A7Graph);

        graph.connect(graph.sourceNode, ISSUE_CREATE, tillIssueCreateNode, ISSUE_CREATE);
        graph.connect(graph.sourceNode, COMMIT_TIME, tillIssueCreateNode, COMMIT_TIME);
        graph.connect(graph.sourceNode, COMMIT_TIME, tillIssueResolveNode, COMMIT_TIME);
        graph.connect(graph.sourceNode, ISSUE_RESOLVE, tillIssueResolveNode, ISSUE_RESOLVE);
        graph.connect(graph.sourceNode, COMMIT_TIME, A7Graph, TimeDiffLowerThanThresholdGraph.COMMIT_TIME);
        graph.connect(graph.sourceNode, ISSUE_RESOLVE, A7Graph, TimeDiffLowerThanThresholdGraph.ISSUE_RESOLVE);
        graph.connect(graph.sourceNode, COMMIT_TIME, inRange, COMMIT_TIME);
        graph.connect(graph.sourceNode, ISSUE_CREATE, inRange, ISSUE_CREATE);
        graph.connect(graph.sourceNode, ISSUE_RESOLVE, inRange, ISSUE_RESOLVE);
        graph.connect(tillIssueCreateNode, A4, graph.sinkNode, A4);
        graph.connect(tillIssueResolveNode, A5, graph.sinkNode, A5);
        graph.connect(inRange, A6, graph.sinkNode, A6);
        graph.connect(A7Graph, TimeDiffLowerThanThresholdGraph.OUTPUT, graph.sinkNode, A7);
        return graph;
    }
}
