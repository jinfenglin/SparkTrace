package core.TraceLabAdaptor.TCML.NativeVertex;

import core.TraceLabAdaptor.dataModel.IO.IOItem;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.graphPipeline.SLayer.SNode;
import org.apache.spark.ml.PipelineStage;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class SNodeBuilder {
    private Map<String, Class> stagesReg;
    private static SNodeBuilder builder;

    protected SNodeBuilder() throws ClassNotFoundException {
        stagesReg = new HashMap<>();
        stagesReg.put("SN_CosineSimilarity", Class.forName("componentRepo.SLayer.featurePipelineStages.VecSimilarity.SparseVecSimilarity.SparseVecCosinSimilarityStage"));
    }

    public static SNodeBuilder getInstance() throws ClassNotFoundException {
        if (builder == null) {
            builder = new SNodeBuilder();
        }
        return builder;
    }


    public SNode buildSNode(TraceLabNode tlNode, String vertexId) throws Exception {
        String nodeLabel = tlNode.getLabel();
        PipelineStage stage = getPipelineStageByLabel(nodeLabel);
        SNode sNode = new SNode(stage, nodeLabel);
        sNode.setVertexId(vertexId);
        for (IOItem item : tlNode.getIOSpec().getInputs()) {
            sNode.addInputField(item.getDef().getFieldName());
        }
        for (IOItem item : tlNode.getIOSpec().getOutputs()) {
            sNode.addOutputField(item.getDef().getFieldName());
        }
        return sNode;
    }

    public PipelineStage getPipelineStageByLabel(String label) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<?> clazz = stagesReg.get(label);
        PipelineStage stage = (PipelineStage) clazz.getConstructor().newInstance();
        return stage;
    }

}
