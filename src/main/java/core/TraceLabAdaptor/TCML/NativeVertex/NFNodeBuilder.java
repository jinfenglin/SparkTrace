package core.TraceLabAdaptor.TCML.NativeVertex;

import componentRepo.FLayer.nativeFlowNodes.Join;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.graphPipeline.FLayer.NFNode;

/**
 *
 */
public class NFNodeBuilder {
    static String JOIN = "NFN_Join";
    private static NFNodeBuilder builder;

    protected NFNodeBuilder() {

    }

    public static NFNodeBuilder getInstance() {
        if (builder == null) {
            builder = new NFNodeBuilder();
        }
        return builder;
    }

    public NFNode buildNFNode(TraceLabNode tlNode, String nodeID) {
        if (tlNode.getLabel().equals(JOIN)) {
            NFNode node = new Join(tlNode.getLabel());
            node.setVertexId(nodeID);
            return node;
        }
        return null;
    }
}
