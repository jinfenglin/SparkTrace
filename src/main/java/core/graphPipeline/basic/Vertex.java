package core.graphPipeline.basic;

import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import org.apache.spark.ml.Pipeline;

import java.util.*;

/**
 *
 */
abstract public class Vertex {
    protected String vertexId;
    protected IOTable inputTable, outputTable;
    protected Vertex context; //If a vertex is included in another vertex, then the second vertex is the context
    protected Map<Symbol, String> symbolValues;

    private void init() {
        vertexId = UUID.randomUUID().toString();
        inputTable = new IOTable(this);
        outputTable = new IOTable(this);
        symbolValues = new HashMap<>();
    }

    public Vertex() {
        init();
    }

    public Vertex(String vertexId) {
        init();
        this.vertexId = vertexId;
    }

    public Vertex(Vertex vertex) {
        this.vertexId = vertex.vertexId;
        inputTable = vertex.inputTable;
        outputTable = vertex.outputTable;
        context = vertex.context;
        symbolValues = vertex.symbolValues;
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

    private void checkSymbolValid(Symbol symbol) {
        assert !(inputTable.containsSymbol(symbol) || outputTable.containsSymbol(symbol));
        assert symbol.getScope().equals(this);
    }

    protected void addSymbol(Symbol symbol, IOTable table) throws Exception {
        checkSymbolValid(symbol);
        IOTableCell cell = new IOTableCell(symbol);
        table.addCell(cell);
        SymbolTable.registerSymbol(symbol);
    }

    public Vertex addInputField(Symbol symbol) throws Exception {
        addSymbol(symbol, inputTable);
        return this;
    }

    public void removeInputField(Symbol symbol) {
        IOTableCell inCell = inputTable.getCellBySymbol(symbol);
        for (IOTableCell inSourceCell : new ArrayList<>(inCell.getInputSource())) {
            inCell.removeInputFrom(inSourceCell);
        }
        inputTable.removeCell(inCell);
    }

    public Vertex addInputField(String symbolName) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        return addInputField(symbol);
    }

    public void removeOutputField(Symbol symbol) {
        IOTableCell outCell = outputTable.getCellBySymbol(symbol);
        for (IOTableCell outTarget : outCell.getOutputTarget()) {
            outCell.removeOutputTo(outTarget);
        }
        outputTable.removeCell(outCell);
    }

    public IOTableCell getInputField(String symbolName) {
        Symbol symbol = new Symbol(this, symbolName);
        return getInputTable().getCellBySymbol(symbol);
    }

    public IOTableCell getOutputField(String symbolName) {
        Symbol symbol = new Symbol(this, symbolName);
        return getOutputTable().getCellBySymbol(symbol);
    }

    public Vertex addOutputField(String symbolName) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        return addOutputField(symbol);
    }

    public Vertex addOutputField(Symbol symbol) throws Exception {
        addSymbol(symbol, outputTable);
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

    public Vertex getContext() {
        return context;
    }

    public void setContext(Vertex context) {
        this.context = context;
    }

    public Map<Symbol, String> getSymbolValues() {
        return symbolValues;
    }

    public void setSymbolValues(Map<Symbol, String> symbolValues) {
        this.symbolValues = symbolValues;
    }
}
