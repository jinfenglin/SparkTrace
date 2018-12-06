package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;
import org.apache.spark.ml.Pipeline;

import java.util.UUID;

/**
 *
 */
abstract public class Vertex {
    protected String vertexId;
    protected IOTable inputTable, outputTable;

    public Vertex() {
        vertexId = UUID.randomUUID().toString();
        inputTable = new IOTable(this);
        outputTable = new IOTable(this);
    }

    public abstract Pipeline toPipeline() throws Exception;

    public Vertex addInputFields(Symbol[] symbols) {
        for (Symbol symbol : symbols) {
            addInputField(symbol);
        }
        return this;
    }

    public Vertex addInputField(Symbol symbol) {
        IOTableCell cell = new IOTableCell(symbol);
        inputTable.addCell(cell);
        return this;
    }

    public Vertex addOutputFields(Symbol[] symbols) {
        for (Symbol symbol : symbols) {
            addOutputField(symbol);
        }
        return this;
    }

    public Vertex addOutputField(Symbol symbol) {
        IOTableCell cell = new IOTableCell(symbol);
        outputTable.addCell(cell);
        return this;
    }

    public Vertex removeInputField(Symbol symbol) {
        if (inputTable.containsSymbol(symbol)) {
            inputTable.removeCell(symbol);
        }
        return this;
    }

    public Vertex removeOutputField(Symbol symbol) {
        if (outputTable.containsSymbol(symbol)) {
            outputTable.removeCell(symbol);
        }
        return this;
    }

    public Vertex removeInputFields(Symbol[] symbols) {
        for (Symbol symbol : symbols) {
            removeInputField(symbol);
        }
        return this;
    }

    public Vertex removeOutputFields(Symbol[] symbols) {
        for (Symbol symbol : symbols) {
            removeOutputField(symbol);
        }
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
