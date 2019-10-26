package componentRepo.FLayer.nativeFlowNodes;

import core.graphPipeline.FLayer.NFNode;

/**
 *
 */
public class FGraphIONode extends NFNode {
    public static String FIO = "FIO";

    public FGraphIONode() {
        super();
        NFNType = FIO;
    }

    public FGraphIONode(String label) {
        super(label);
        NFNType = FIO;
    }

    @Override
    public String nodeContentInfo() {
        return getNFNodeType(); //have no non io parameters
    }

    @Override
    public boolean isIONode() {
        return true;
    }


}
