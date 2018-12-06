package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;

import java.util.ArrayList;
import java.util.List;

/**
 * A cell in the IOTable.
 */
public class IOTableCell {
    private List<IOTableCell> inputSource, outputTarget; //Record the connected cells
    Symbol fieldSymbol;

    public IOTableCell(Symbol symbol) {
        inputSource = new ArrayList<>();
        outputTarget = new ArrayList<>();
        fieldSymbol = symbol;
    }

    public void sendOutputTo(IOTableCell targetCell) {
        outputTarget.add(targetCell);
        targetCell.getInputSource().add(this);
    }

    public void receiveInputFrom(IOTableCell inputCell) {
        inputSource.add(inputCell);
        inputCell.getOutputTarget().add(this);
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
}
