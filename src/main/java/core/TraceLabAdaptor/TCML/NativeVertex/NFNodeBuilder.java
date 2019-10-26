package core.TraceLabAdaptor.TCML.NativeVertex;

import componentRepo.FLayer.nativeFlowNodes.IDF;
import componentRepo.FLayer.nativeFlowNodes.Join;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.graphPipeline.FLayer.NFNode;

/**
 *
 */
public class NFNodeBuilder {

    private static NFNodeBuilder builder;

    protected NFNodeBuilder() {

    }

    public static NFNodeBuilder getInstance() {
        if (builder == null) {
            builder = new NFNodeBuilder();
        }
        return builder;
    }

    public NFNode buildNFNode(TraceLabNode tlNode, String nodeID) throws Exception {
        if (tlNode.getLabel().equals(Join.JOIN)) {
            Join node = new Join(tlNode.getLabel());
            node.setVertexId(nodeID);
            return node;
        } else if (tlNode.getLabel().equals(IDF.IDF)) {
            IDF node = new IDF(tlNode.getLabel());
            node.setVertexId(nodeID);
            return node;
        }
        return null;
    }
}
