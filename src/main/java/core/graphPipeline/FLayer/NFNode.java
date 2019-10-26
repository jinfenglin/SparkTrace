package core.graphPipeline.FLayer;

/**
 * Native Flow Node (NFNode) refer to the node native to the workflow layers. Operations such as JOIN,FILTER all belong
 * to this category. This type of node is knowns as horizontal operation node that only manipulate the rows.
 * It contains code directly calling Spark operands. Usually, this type of node don't need schema information.
 */
public abstract class NFNode extends FNode {
    protected static String NFNType;

    public NFNode(String label) {
        super(label);
    }

    public NFNode() {
        super();
    }

    public String getNFNodeType() {
        return NFNType;
    }

    /**
     * NFNNodes are not IO nodes unless explicitly specified as FGraphIONode
     *
     * @return
     */
    @Override
    public boolean isIONode() {
        return false;
    }
}
