package core.graphPipeline.FLayer;

import core.graphPipeline.basic.Node;

abstract public class FNode extends Node implements FLayerComponent {
    public FNode() {
        super();
    }

    public FNode(String label) {
        super(label);
    }
}
