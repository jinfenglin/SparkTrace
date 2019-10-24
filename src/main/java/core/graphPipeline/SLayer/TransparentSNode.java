package core.graphPipeline.SLayer;

import core.graphPipeline.basic.IOTable;
import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.ITransparentVertex;
import core.graphPipeline.graphSymbol.SymbolTable;
import org.apache.spark.ml.PipelineStage;

/**
 *
 */
public class TransparentSNode extends SNode implements ITransparentVertex {
    public TransparentSNode(PipelineStage pipelineStage) {
        super(pipelineStage);
    }

    public TransparentSNode(PipelineStage pipelineStage, String label) {
        super(pipelineStage, label);
    }

    @Override
    public IOTableCell getRelativeInputFiled(IOTableCell outputCell) throws Exception {
        return getRelativeFiled(outputCell, outputTable, inputTable);
    }

    @Override
    public IOTableCell getRelativeOutputField(IOTableCell inputField) throws Exception {
        return getRelativeFiled(inputField, inputTable, outputTable);
    }

    /**
     * Find index of the cell in reside table and then get the element with same index in search table
     *
     * @param cell
     * @param resideTable
     * @param searchTable
     * @return
     */
    private IOTableCell getRelativeFiled(IOTableCell cell, IOTable resideTable, IOTable searchTable) throws Exception {
        assert resideTable.getCells().size() == searchTable.getCells().size();
        for (int i = 0; i < resideTable.getCells().size(); i++) {
            if (cell.equals(resideTable.getCells().get(i))) {
                return searchTable.getCells().get(i);
            }
        }
        throw new Exception(String.format("OutputCell not found in TransparentSNode %s", this.getVertexId()));
    }

    /**
     * Copy the symbol value from one table to another
     */
    private void mathIOTable(IOTable from, IOTable to) throws Exception {
        assert from.getCells().size() == to.getCells().size();
        for (int i = 0; i < from.getCells().size(); i++) {
            IOTableCell inputCell = from.getCells().get(i);
            IOTableCell outputCell = to.getCells().get(i);
            SymbolTable.shareSymbolValue(inputCell.getFieldSymbol(), outputCell.getFieldSymbol());
        }
    }

    @Override
    public void matchOutputToInput() throws Exception {
        mathIOTable(outputTable, inputTable);
    }

    @Override
    public void matchInputToOutput() throws Exception {
        mathIOTable(inputTable, outputTable);
    }
}
