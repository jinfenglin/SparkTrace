package componentRepo.FLayer.nativeFlowNodes;


import core.graphPipeline.FLayer.NFNode;

/**
 *
 */
public class IDF extends NFNode {
    public static String IDF = "NFN_IDF";

    public IDF(String label) throws Exception {
        super(label);
        NFNType = IDF;
        addInputField("sourceInputDataset");
        addInputField("targetInputDataset");
        addOutputField("sourceOutputDataset");
        addOutputField("targetOutputDataset");
    }

    @Override
    public String nodeContentInfo() {
        return vertexId;
    }

}
