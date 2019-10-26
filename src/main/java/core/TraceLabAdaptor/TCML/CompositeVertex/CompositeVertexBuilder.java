package core.TraceLabAdaptor.TCML.CompositeVertex;

import core.TraceLabAdaptor.dataModel.IO.IOItemDefinition;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.TraceLabAdaptor.dataModel.TraceLabEdge;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.graphPipeline.basic.Vertex;

import java.util.List;
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

    protected List<TraceLabEdge> getTraceLabEdgeWithoutStartAndEnd() {
        List<TraceLabEdge> edges = tc.getEdges();
        edges = edges.stream().filter(x -> !x.getSource().equals(START) && !x.getTarget().equals(START)
                && !x.getSource().equals(END) && !x.getTarget().equals(END)).collect(Collectors.toList()); //remove edges links to start and end node
        return edges;
    }
}
