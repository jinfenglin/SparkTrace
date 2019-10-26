package core.TraceLabAdaptor.TCML.CompositeVertex;

import core.TraceLabAdaptor.dataModel.IO.IOItem;
import core.TraceLabAdaptor.dataModel.IO.IOItemDefinition;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.TraceLabAdaptor.dataModel.TraceLabEdge;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.graphPipeline.basic.Graph;
import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.Vertex;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class CompositeVertexBuilder {
    static String START = "Start", END = "End";
    TraceComposite tc;
    String vertexId;

    public CompositeVertexBuilder(TraceComposite tc, String vertexId) {
        this.tc = tc;
        this.vertexId = vertexId;
    }

    protected void addIOField(Vertex v) throws Exception {
        for (IOItemDefinition inputField : tc.getInputs()) {
            v.addInputField(inputField.getFieldName());
        }
        for (IOItemDefinition outputField : tc.getOutputs()) {
            v.addOutputField(outputField.getFieldName());
        }
    }

    protected List<TraceLabNode> getTraceLabNodeWithoutStartAndEnd() {
        List<TraceLabNode> vertices = tc.getVertices();
        vertices = vertices.stream().filter(x -> !x.getLabel().equals(START) && !x.getLabel().equals(END)).collect(Collectors.toList()); //remove start node and end node
        return vertices;
    }

    protected TraceLabNode getFirstNodeByLabel(String label) {
        List<TraceLabNode> vertices = tc.getVertices();
        return vertices.stream().filter(x -> x.getLabel().equals(label)).findFirst().get(); //remove start node and end node
    }


    protected List<TraceLabEdge> getTraceLabEdgeWithoutStartAndEnd() {
        List<TraceLabEdge> edges = tc.getEdges();
        edges = edges.stream().filter(x -> !x.getSource().equals(START) && !x.getTarget().equals(START)
                && !x.getSource().equals(END) && !x.getTarget().equals(END)).collect(Collectors.toList()); //remove edges links to start and end node
        return edges;
    }

    protected List<TraceLabEdge> getTraceLabEdgeWithStartAndEnd() {
        List<TraceLabEdge> edges = tc.getEdges();
        edges = edges.stream().filter(x -> x.getSource().equals(START) || x.getTarget().equals(START)
                || x.getSource().equals(END) || x.getTarget().equals(END)).collect(Collectors.toList()); //remove edges links to start and end node
        return edges;
    }

    protected void connectNonIONodes(Graph g, Map<String, TraceLabNode> TLNodeIndex) throws Exception {
        List<TraceLabEdge> edges = getTraceLabEdgeWithoutStartAndEnd();
        for (TraceLabEdge edge : edges) {
            Vertex sVertex = g.getNode(edge.getSource());
            Vertex tVertex = g.getNode(edge.getTarget());
            TraceLabNode sTLNode = TLNodeIndex.get(edge.getSource());
            TraceLabNode tTLNode = TLNodeIndex.get(edge.getTarget());

            for (IOItem sItem : sTLNode.getIOSpec().getOutputs()) {
                for (IOItem tItem : tTLNode.getIOSpec().getInputs()) {
                    if (sItem.isMatch(tItem)) {
                        g.connect(sVertex, sItem.getDef().getFieldName(), tVertex, tItem.getDef().getFieldName());
                    }
                }
            }
        }
    }

    protected void connectIONodes(Graph g, Map<String, TraceLabNode> TLNodeIndex) throws Exception {
        List<TraceLabEdge> ioEdges = getTraceLabEdgeWithStartAndEnd();
        for (TraceLabEdge edge : ioEdges) {
            if (edge.getSource().equals(START) && TLNodeIndex.containsKey(edge.getTarget())) {
                TraceLabNode targetNode = TLNodeIndex.get(edge.getTarget());
                for (IOTableCell cell : g.sourceNode.getOutputTable().getCells()) {
                    for (IOItem tItem : targetNode.getIOSpec().getInputs()) {
                        String symbolName = cell.getFieldSymbol().getSymbolName();
                        if (symbolName.equals(tItem.getMapTo())) {
                            g.connect(g.sourceNode, symbolName, g.getNode(targetNode.getNodeId()), tItem.getDef().getFieldName());
                        }
                    }
                }
            } else if (TLNodeIndex.containsKey(edge.getSource()) && edge.getTarget().equals(END)) {
                TraceLabNode sourceNode = TLNodeIndex.get(edge.getSource());
                for (IOTableCell cell : g.sinkNode.getInputTable().getCells()) {
                    for (IOItem sItem : sourceNode.getIOSpec().getOutputs()) {
                        String symbolName = cell.getFieldSymbol().getSymbolName();
                        if (symbolName.equals(sItem.getMapTo())) {
                            g.connect(g.getNode(sourceNode.getNodeId()), sItem.getDef().getFieldName(), g.sinkNode, symbolName);
                        }
                    }
                }
            } else {
                throw new Exception(String.format("%s is not properly connected...", edge.toString()));
            }
        }
    }
}
