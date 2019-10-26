package core.TraceLabAdaptor.TCML.CompositeVertex;

import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.graphPipeline.SLayer.SGraph;

/**
 *
 */
public class SGraphBuilder extends CompositeVertexBuilder {
    public SGraphBuilder(TraceComposite tc, String vertexId) {
        super(tc, vertexId);
    }

    public SGraph buildSGraph() throws Exception {
        SGraph sg = new SGraph(vertexId);
        addIOField(sg);
        return sg;
    }
}
