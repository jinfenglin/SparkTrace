package core.TraceLabAdaptor.TCML.CompositeVertex;

import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.TraceLabAdaptor.dataModel.TraceLabNodeUtils;
import core.graphPipeline.SLayer.SGraph;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SGraphBuilder extends CompositeVertexBuilder {
    public SGraphBuilder(TraceComposite tc, String vertexId) {
        super(tc, vertexId);
    }

    public SGraph buildSGraph() throws Exception {
        SGraph sg = new SGraph(tc.getLabel());
        sg.setVertexId(vertexId);
        addIOField(sg);
        //create main body without start and end nodes/edges
        List<TraceLabNode> vertices = getTraceLabNodeWithoutStartAndEnd();


        Map<String, TraceLabNode> TLNodeIndex = new HashMap<>();
        for (TraceLabNode node : vertices) {
            TLNodeIndex.put(node.getNodeId(), node);
            sg.addNode(TraceLabNodeUtils.toSparkGraphVertex(node));
        }
        connectNonIONodes(sg, TLNodeIndex);
        connectIONodes(sg,TLNodeIndex);
        return sg;
    }
}
