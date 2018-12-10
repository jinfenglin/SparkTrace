package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;
import core.GraphSymbol.SymbolTable;
import org.apache.spark.ml.Pipeline;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
abstract public class Vertex {
    protected String vertexId;
    protected IOTable inputTable, outputTable;

    public Vertex() {
        vertexId = UUID.randomUUID().toString();
        inputTable = new IOTable(this, IOTable.TableType.INPUT_TABLE);
        outputTable = new IOTable(this, IOTable.TableType.OUTPUT_TABLE);
    }

    /**
     * Get vertices which provide in degree to the vertex
     *
     * @return
     */
    public Set<Vertex> getInputVertices() {
        Set<Vertex> inputNodes = new HashSet<>();
        for (IOTableCell cell : inputTable.getCells()) {
            for (IOTableCell sourceCell : cell.getInputSource()) {
                inputNodes.add(sourceCell.getFieldSymbol().getScope());
            }
        }
        return inputNodes;
    }

    /**
     * Get vertices which provide out degree to the vertex
     *
     * @return
     */
    public Set<Vertex> getOutputVertices() {
        Set<Vertex> outputNodes = new HashSet<>();
        for (IOTableCell cell : outputTable.getCells()) {
            for (IOTableCell targetCell : cell.getOutputTarget()) {
                outputNodes.add(targetCell.getFieldSymbol().getScope());
            }
        }
        return outputNodes;
    }

    public abstract Pipeline toPipeline() throws Exception;

    public Vertex addInputFields(Symbol[] symbols) throws Exception {
        for (Symbol symbol : symbols) {
            addInputField(symbol);
        }
        return this;
    }

    public Vertex addInputField(Symbol symbol) throws Exception {
        IOTableCell cell = new IOTableCell(inputTable, symbol);
        inputTable.addCell(cell);
        SymbolTable.registerInputSymbol(symbol);
        return this;
    }

    public Vertex addInputField(String symbolName) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        return addInputField(symbol);
    }

    public IOTableCell getInputField(String symbolName) {
        Symbol symbol = new Symbol(this, symbolName);
        return getInputTable().getCellBySymbol(symbol);
    }

    public Vertex addOutputFields(Symbol[] symbols) throws Exception {
        for (Symbol symbol : symbols) {
            addOutputField(symbol);
        }
        return this;
    }

    public Vertex addOutputField(String symbolName) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        return addOutputField(symbol);
    }

    public IOTableCell getOutputField(String symbolName) {
        Symbol symbol = new Symbol(this, symbolName);
        return getOutputTable().getCellBySymbol(symbol);
    }

    public Vertex addOutputField(Symbol symbol) throws Exception {
        IOTableCell cell = new IOTableCell(outputTable, symbol);
        outputTable.addCell(cell);
        SymbolTable.registerOutputSymbol(symbol);
        return this;
    }


    public IOTable getInputTable() {
        return inputTable;
    }

    public void setInputTable(IOTable inputTable) {
        this.inputTable = inputTable;
    }

    public IOTable getOutputTable() {
        return outputTable;
    }

    public void setOutputTable(IOTable outputTable) {
        this.outputTable = outputTable;
    }

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "vertexId='" + vertexId + '\'' +
                '}';
    }

    public void setId(String id) {
        vertexId = id;
    }
}
