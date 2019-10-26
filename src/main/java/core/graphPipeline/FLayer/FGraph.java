package core.graphPipeline.FLayer;

import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.graphPipeline.basic.Graph;

import java.util.HashMap;
import java.util.HashSet;

public class FGraph extends Graph {
    FSchemaManger fsManger; //Control the Schema for this and child FGraph

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
        fsManger = new FSchemaManger();
    }

    /**
     * Build FGraph based on TraceComposite
     *
     * @param tc
     */
    public FGraph(TraceComposite tc) {
        super();
    }
}
