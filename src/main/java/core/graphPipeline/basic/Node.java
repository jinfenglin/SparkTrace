package core.graphPipeline.basic;

/**
 *
 */
public abstract class Node extends Vertex {
    public Node() {
        super();
    }

    public Node(String label) {
        super(label);
    }

    public abstract String nodeContentInfo();

    public abstract boolean isIONode();
}
