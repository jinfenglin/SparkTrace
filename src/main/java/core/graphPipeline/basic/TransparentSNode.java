package core.graphPipeline.basic;

import core.graphPipeline.graphSymbol.SymbolTable;
import org.apache.spark.ml.PipelineStage;

/**
 * A transparentSNode including infusionNode and IOStages
 */
public class TransparentSNode extends SNode implements ITransparentVertex {
    public TransparentSNode(PipelineStage pipelineStage) {
        super(pipelineStage);
    }

    public TransparentSNode(PipelineStage pipelineStage, String nodeId) {
        super(pipelineStage, nodeId);
    }

    @Override
    public void matchOutputToInput() {
        assert inputTable.getCells().size() == outputTable.getCells().size();
        for (int i = 0; i < inputTable.getCells().size(); i++) {
            IOTableCell inputCell = inputTable.getCells().get(i);
            IOTableCell outputCell = outputTable.getCells().get(i);
            SymbolTable.setInputSymbolValue(inputCell.getFieldSymbol(), SymbolTable.getOutputSymbolValue(outputCell.getFieldSymbol()));
        }
    }

    @Override
    public void matchInputToOutput() {
        assert inputTable.getCells().size() == outputTable.getCells().size();
        for (int i = 0; i < inputTable.getCells().size(); i++) {
            IOTableCell inputCell = inputTable.getCells().get(i);
            IOTableCell outputCell = outputTable.getCells().get(i);
            SymbolTable.setOutputSymbolValue(outputCell.getFieldSymbol(), SymbolTable.getInputSymbolValue(inputCell.getFieldSymbol()));
        }
    }
}
