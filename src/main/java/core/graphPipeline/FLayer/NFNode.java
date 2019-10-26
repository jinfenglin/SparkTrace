package core.graphPipeline.FLayer;

/**
 * Native Flow Node (NFNode) refer to the node native to the workflow layers. Operations such as JOIN,FILTER all belong
 * to this category. This type of node is knowns as horizontal operation node that only manipulate the rows.
 * It contains code directly calling Spark operands. Usually, this type of node don't need schema information.
 */
public class NFNode extends FNode {
    public NFNode(String nodeId) {
        super(nodeId);
    }
}
