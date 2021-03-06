package componentRepo.FLayer.nativeFlowNodes;

import core.graphPipeline.FLayer.NFNode;

/**
 * Check  NFN_Join in TraceLabWorkSpace/src/components/workflow/datasetOperator/
 */
public class Join extends NFNode {
    public static String JOIN = "NFN_Join";

    public Join(String label) throws Exception {
        super(label);
        NFNType = JOIN;
        addInputField("sourceInputDataset");
        addInputField("targetInputDataset");
        addOutputField("OutputDataset");
    }

    @Override
    public String nodeContentInfo() {
        return vertexId; //
    }

}
