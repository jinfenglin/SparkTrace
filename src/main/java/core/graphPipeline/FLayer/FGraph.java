package core.graphPipeline.FLayer;

import core.graphPipeline.basic.Edge;
import core.graphPipeline.basic.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FGraph extends Vertex{
    private Map<String, Vertex> nodes;
    private Set<Edge> edges; //Record the node level connection, the field level connection is recorded by the IOTable
    public FNode sourceNode, sinkNode;
    private Map<String, String> config;

    public FGraph() {
        super();
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new FNode();
        sinkNode = new FNode();
        sourceNode.setVertexLabel("SourceNode");
        sinkNode.setVertexLabel("SinkNode");
        sourceNode.setContext(this);
        sinkNode.setContext(this);
        nodes.put(sourceNode.getVertexId(), sourceNode);
        nodes.put(sinkNode.getVertexId(), sinkNode);
        config = new HashMap<>();
    }
}
