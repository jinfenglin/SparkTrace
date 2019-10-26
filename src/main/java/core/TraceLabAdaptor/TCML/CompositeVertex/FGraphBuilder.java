package core.TraceLabAdaptor.TCML.CompositeVertex;

import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.TraceLabAdaptor.dataModel.TraceLabEdge;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.TraceLabAdaptor.dataModel.TraceLabNodeUtils;
import core.graphPipeline.FLayer.FGraph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class FGraphBuilder extends CompositeVertexBuilder {
    public FGraphBuilder(TraceComposite tc, String vertexId) {
        super(tc, vertexId);
    }

    //TODO add function for Fschema manager
    public FGraph buildFGraph() throws Exception {
        FGraph fg = new FGraph();
        fg.setVertexId(vertexId);
        addIOField(fg);

        List<TraceLabNode> vertices = getTraceLabNodeWithoutStartAndEnd();
        Map<String, TraceLabNode> TLNodeIndex = new HashMap<>();
        for (TraceLabNode node : vertices) {
            fg.addNode(TraceLabNodeUtils.toSparkGraphVertex(node));
            TLNodeIndex.put(node.getNodeId(), node);
        }
        connectNonIONodes(fg, TLNodeIndex);
        connectIONodes(fg, TLNodeIndex);
        return fg;
    }
}
