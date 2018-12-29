package core.graphPipeline.basic;

import core.graphPipeline.graphSymbol.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * A cell in the IOTable.
 */
public class IOTableCell {
    private List<IOTableCell> inputSource, outputTarget; //Record the connected cells
    private Symbol fieldSymbol;
    private IOTable parentTable;
    private boolean removable; //whether this filed can be removed or not, now used only in penetration as a short cut

    public IOTableCell(Symbol symbol) {
        inputSource = new ArrayList<>();
        outputTarget = new ArrayList<>();
        fieldSymbol = symbol;
        removable = true;
    }

    public void sendOutputTo(IOTableCell targetCell) {
        outputTarget.add(targetCell);
        targetCell.getInputSource().add(this);

    }

    public void receiveInputFrom(IOTableCell inputCell) {
        inputSource.add(inputCell);
        inputCell.getOutputTarget().add(this);
    }

    /**
     * Remove the connection sent to the give target cell;
     *
     * @param targetCell
     */
    public void removeOutputTo(IOTableCell targetCell) {
        outputTarget.remove(targetCell);
        targetCell.getInputSource().remove(this);
    }

    public void removeInputFrom(IOTableCell inputCell) {
        inputSource.add(inputCell);
        inputCell.getOutputTarget().remove(this);
    }


    public List<IOTableCell> getInputSource() {
        return inputSource;
    }

    public void setInputSource(List<IOTableCell> inputSource) {
        this.inputSource = inputSource;
    }

    public List<IOTableCell> getOutputTarget() {
        return outputTarget;
    }

    public void setOutputTarget(List<IOTableCell> outputTarget) {
        this.outputTarget = outputTarget;
    }

    public Symbol getFieldSymbol() {
        return fieldSymbol;
    }

    public void setFieldSymbol(Symbol fieldSymbol) {
        this.fieldSymbol = fieldSymbol;
    }

    public IOTable getParentTable() {
        return parentTable;
    }

    public void setParentTable(IOTable parentTable) {
        this.parentTable = parentTable;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("Cell_Symbol:" + fieldSymbol.toString());
        builder.append(" | ");
        builder.append(" From_Symbol:");
        StringJoiner j1 = new StringJoiner(",");
        for (IOTableCell cell : inputSource) {
            j1.add(cell.getFieldSymbol().toString());
        }
        builder.append(j1.toString());
        builder.append(" | ");
        builder.append(" To_Symbol:");
        StringJoiner j2 = new StringJoiner(",");
        for (IOTableCell cell : outputTarget) {
            j2.add(cell.getFieldSymbol().toString());
        }
        builder.append(j2.toString());
        builder.append(" | ");
        builder.append("}");
        return builder.toString();
    }

    public boolean isRemovable() {
        return removable;
    }

    public void setRemovable(boolean removable) {
        this.removable = removable;
    }
}
