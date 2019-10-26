package core.graphPipeline.FLayer;

/**
 * FLayer component type
 */
public enum FType {
    NFNode, //Native Flow Node. E.g. JOIN operation  Refer NFNode class
    CFNode, // Composite Flow Node. Refer CFNode class
    FGraph,// Flow graph
    SGraph,//
    SNode//
    ;

    public static FType fromString(String text) throws Exception {
        text = text.toUpperCase();
        if (text.startsWith("CFN")) {
            return CFNode;
        } else if (text.startsWith("NFN")) {
            return NFNode;
        } else if (text.startsWith("FG")) {
            return FGraph;
        } else if (text.startsWith("SG")) {
            return SGraph;
        } else if (text.startsWith("SN")) {
            return SNode;
        }
        throw new Exception(String.format("%s belong to no type of component", text));
    }
}
