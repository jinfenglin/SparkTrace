package core.graphPipeline.basic;

import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.SLayer.SNode;
import featurePipelineStages.SGraphIOStage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class Graph extends Vertex{
    private Map<String, Vertex> nodes;
    private Set<Edge> edges; //Record the node level connection, the field level connection is recorded by the IOTable
    public Vertex sourceNode, sinkNode;
    private Map<String, String> config;


    public Graph() {
        super();
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new SNode(new SGraphIOStage());
        sinkNode = new SNode(new SGraphIOStage());
        sourceNode.setVertexLabel("SourceNode");
        sinkNode.setVertexLabel("SinkNode");
        sourceNode.setContext(this);
        sinkNode.setContext(this);
        nodes.put(sourceNode.getVertexId(), sourceNode);
        nodes.put(sinkNode.getVertexId(), sinkNode);
        config = new HashMap<>();

    }
}
