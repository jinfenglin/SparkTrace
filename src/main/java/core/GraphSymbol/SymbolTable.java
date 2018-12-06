package core.GraphSymbol;

import core.pipelineOptimizer.Vertex;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class SymbolTable {
    private Map<Vertex, Map<String, Symbol>> symbolMap;
    private static SymbolTable symbolTable;

    private SymbolTable() {
        symbolMap = new HashMap<>();
    }

    public SymbolTable getInstance() {
        if (symbolTable == null) {
            symbolTable = new SymbolTable();
        }
        return symbolTable;
    }


    public static Symbol getSymbol(Vertex scope, String varName) {
        return null;
    }


    public static void registerSymbol(Symbol symbol) {

    }
}
