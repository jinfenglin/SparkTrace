package core.graphPipeline.FLayer;

import componentRepo.FLayer.nativeFlowNodes.FGraphIONode;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.graphPipeline.basic.Graph;
import core.graphPipeline.basic.Vertex;

import java.util.HashMap;
import java.util.HashSet;

public class FGraph extends Graph implements FLayerComponent {
    FSchemaManger fsManger; //Control the Schema for this and child FGraph

    public FGraph() throws Exception {
        super();
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new FGraphIONode();
        sinkNode = new FGraphIONode();
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

    @Override
    public void addNode(Vertex node) throws Exception {
        if (node instanceof FLayerComponent) {
            super.addNode(node);
        } else {
            throw new Exception(String.format("%s is not a FLayer component, " +
                    "therefore can not be added to FGraph. Only FNode and FGraph are allowed.", node.getVertexLabel()));
        }
    }
}
