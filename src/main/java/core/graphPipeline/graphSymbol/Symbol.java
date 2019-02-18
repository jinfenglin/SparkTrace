package core.graphPipeline.graphSymbol;

import core.graphPipeline.basic.Vertex;

/**
 * A symbol used in the SGraph. A symbol name should be unique within a scope and a scope is a graph/node where
 * this symbols resides. The symbol is immutable, and global just like String.
 */
public class Symbol {
    private final Vertex scope; //The graph/node this symbol resides
    private final String symbolName; // The name of symbol in the scope which should be locally unique

    public Symbol(Vertex vertex, String symbolName) {
        this.scope = vertex;
        this.symbolName = symbolName;
    }

    public Vertex getScope() {
        return scope;
    }

    public String getSymbolName() {
        return symbolName;
    }

    public String getSymbolValue() {
        if (scope.getInputTable().containsSymbol(this)) {
            return SymbolTable.getSymbolValue(this);
        } else if (scope.getOutputTable().containsSymbol(this)) {
            return SymbolTable.getSymbolValue(this);
        }
        return null;
    }

    @Override
    public String toString() {
        return scope.getVertexId() + "." + symbolName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Symbol symbol = (Symbol) o;

        if (!scope.equals(symbol.scope)) return false;
        return symbolName.equals(symbol.symbolName);
    }

    @Override
    public int hashCode() {
        int result = scope.hashCode();
        result = 31 * result + symbolName.hashCode();
        return result;
    }
}
