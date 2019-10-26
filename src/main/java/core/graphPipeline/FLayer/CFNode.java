package core.graphPipeline.FLayer;


import core.graphPipeline.SLayer.SGraph;

/**
 * Composite Flow Node (CFNode) refer to the workflow nodes composited by SGraphs and SNode. This type of node accept
 * one dataset as input and output one dataset. This type node is also known as vertical operation nodes which manipulate
 * columns and dataset schema.
 * It requires a dataset schema to configure the internal SGraph.
 * //TODO add computation ability and pass FSchema
 */
public class CFNode extends FNode {
    SGraph sGraph;
    FSchema inputSchema, outputSchema;

    public CFNode(String id) throws Exception {
        super(id);
    }

    public SGraph getsGraph() {
        return sGraph;
    }

    public void setsGraph(SGraph sGraph) {
        this.sGraph = sGraph;
    }
}
