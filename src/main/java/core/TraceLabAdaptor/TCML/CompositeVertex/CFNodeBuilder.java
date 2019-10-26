package core.TraceLabAdaptor.TCML.CompositeVertex;

import core.TraceLabAdaptor.dataModel.IO.IOItem;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.TraceLabAdaptor.dataModel.TraceLabEdge;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.TraceLabAdaptor.dataModel.TraceLabNodeUtils;
import core.graphPipeline.FLayer.CFNode;
import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.basic.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Build a CFNode given a TraceComposite object restored in a TCML file
 */
public class CFNodeBuilder extends CompositeVertexBuilder {
    static String SOURCE_NODE_LABEL = "SourceNode", SINK_NODE_LABEL = "SinkNode";

    public CFNodeBuilder(TraceComposite tc, String cfNodeId) {
        super(tc, cfNodeId);
    }

    public CFNode buildCFNode() throws Exception {
        CFNode cfNode = new CFNode(tc.getName());
        addIOField(cfNode);
        cfNode.setVertexId(vertexId);
        cfNode.setsGraph(buildSGraph());
        return cfNode;
    }

    private SGraph buildSGraph() throws Exception {
        SGraph sg = new SGraph();
        // Create IONode
        TraceLabNode sourceTLNode = findSourceNode();
        sg.sourceNode.setVertexId(sourceTLNode.getNodeId());
        for (IOItem item : sourceTLNode.getIOSpec().getOutputs()) {
            String fieldName = item.getDef().getFieldName();
            sg.addInputField(fieldName);
        }
        TraceLabNode sinkTLNode = findSinkNode();
        sg.sinkNode.setVertexId(sinkTLNode.getNodeId());
        for (IOItem item : sinkTLNode.getIOSpec().getInputs()) {
            String fieldName = item.getDef().getFieldName();
            sg.addOutputField(fieldName);
        }
        List<TraceLabNode> vertices = getTraceLabNodeWithoutStartAndEnd();
        List<TraceLabEdge> edges = getTraceLabEdgeWithoutStartAndEnd();

        Map<String, TraceLabNode> TLNodeIndex = new HashMap<>();
        for (TraceLabNode node : vertices) {
            TLNodeIndex.put(node.getNodeId(), node);
            if (!node.equals(sourceTLNode) && !node.equals(sinkTLNode)) {
                sg.addNode(TraceLabNodeUtils.toSparkGraphVertex(node));
            }
        }
        for (TraceLabEdge edge : edges) {
            Vertex sVertex = sg.getNode(edge.getSource());
            Vertex tVertex = sg.getNode(edge.getTarget());
            TraceLabNode sTLNode = TLNodeIndex.get(edge.getSource());
            TraceLabNode tTLNode = TLNodeIndex.get(edge.getTarget());

            for (IOItem sItem : sTLNode.getIOSpec().getOutputs()) {
                for (IOItem tItem : tTLNode.getIOSpec().getInputs()) {
                    if (sItem.isMatch(tItem)) {
                        sg.connect(sVertex, sItem.getDef().getFieldName(), tVertex, tItem.getDef().getFieldName());
                    }
                }
            }
        }
        return sg;
    }

    private List<TraceLabNode> findVertexWithLabelStartWith(String subStr) {
        return tc.getVertices().stream().filter(x -> x.getLabel().startsWith(subStr)).collect(Collectors.toList());
    }

    public TraceLabNode findSourceNode() throws Exception {
        List<TraceLabNode> res = findVertexWithLabelStartWith(SOURCE_NODE_LABEL);
        if (res.size() != 1) {
            throw new Exception("CFNode should have exactly one source node");
        }
        return res.get(0);
    }

    public TraceLabNode findSinkNode() throws Exception {
        List<TraceLabNode> res = findVertexWithLabelStartWith(SINK_NODE_LABEL);
        if (res.size() != 1) {
            throw new Exception("CFNode should have exactly one target node");
        }
        return res.get(0);
    }
}
