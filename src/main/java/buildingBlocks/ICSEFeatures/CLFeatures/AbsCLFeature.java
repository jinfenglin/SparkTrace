package buildingBlocks.ICSEFeatures.CLFeatures;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.cloestLinkedCommit.FindClosestPreviousLinkedCommit;

/**
 *
 */
public abstract class AbsCLFeature {
    public static String COMMIT_ID = "COMMIT_ID", COMMIT_DATE = "COMMIT_DATE", CL = "CL", LINKED_COMMIT = "LINKED_COMMIT";

    public static SNode createFindClosestLink(boolean findPrevious) throws Exception {
        FindClosestPreviousLinkedCommit fcpl = new FindClosestPreviousLinkedCommit();
        fcpl.set("isPreviousClosest", findPrevious);
        SNode findCL = new SNode(fcpl, "findCL");
        findCL.addInputField(COMMIT_ID);
        findCL.addInputField(COMMIT_DATE);
        findCL.addInputField(LINKED_COMMIT);
        findCL.addOutputField(CL);
        return findCL;
    }

    public static void connectFindClosestLink(SGraph graph, SNode findCL) throws Exception {
        graph.connect(graph.sourceNode, COMMIT_ID, findCL, COMMIT_ID);
        graph.connect(graph.sourceNode, COMMIT_DATE, findCL, COMMIT_DATE);
        graph.connect(graph.sourceNode, LINKED_COMMIT, findCL, LINKED_COMMIT);
    }

}
