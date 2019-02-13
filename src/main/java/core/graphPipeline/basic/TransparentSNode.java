package core.graphPipeline.basic;

import core.graphPipeline.graphSymbol.SymbolTable;
import org.apache.spark.ml.PipelineStage;

/**
 *
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
    public IOTableCell getRelativeInputFiled(IOTableCell outputCell) throws Exception {
        assert inputTable.getCells().size() == outputTable.getCells().size();
        for (int i = 0; i < inputTable.getCells().size(); i++) {
            if (outputCell.equals(outputTable.getCells().get(i))) {
                return inputTable.getCells().get(i);
            }
        }
        throw new Exception(String.format("OutputCell not found in TransparentSNode %s", this.getVertexId()));
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
