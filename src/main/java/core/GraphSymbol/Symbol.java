package core.GraphSymbol;

import core.pipelineOptimizer.Vertex;

/**
 * A symbol used in the SGraph. A symbol name should be unique within a scope and a scope is a graph/node where
 * this symbols resides.
 */
public class Symbol {
    private Vertex scope; //The graph/node this symbol resides
    private String symbolName; // The name of symbol in the scope which should be locally unique
    private String symbolValue; // The value of symbol that will be converted into column name in dataset, this should be globally unique

    public Symbol(Vertex vertex, String symbolName) {
        this.scope = vertex;
        this.symbolName = symbolName;
        this.symbolValue = symbolName; //The symbolValue same as its symbol name in default, but will be changed when conflict happens
    }

    public Vertex getScope() {
        return scope;
    }

    public void setScope(Vertex scope) {
        this.scope = scope;
    }

    public String getSymbolName() {
        return symbolName;
    }

    public void setSymbolName(String symbolName) {
        this.symbolName = symbolName;
        this.symbolValue = symbolName;
    }

    public String getSymbolValue() {
        return symbolValue;
    }

    @Override
    public String toString() {
        return "Symbol{" +
                "scope=" + scope +
                ", symbolName='" + symbolName + '\'' +
                ", symbolValue='" + symbolValue + '\'' +
                '}';
    }
}
