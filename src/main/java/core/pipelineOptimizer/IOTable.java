package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * IOTable recorded the mapping relationships between two SNode. The IOTableCells will connect to another IOTableCell.In
 * each SNode there will be one input table and one output table;
 */
public class IOTable {
    private Vertex context; //IOTable must reside within a vertex
    private Map<Symbol, IOTableCell> cells; //A HahsMap for fast access to IOTableCell

    public IOTable(Vertex context) {
        this.context = context;
        cells = new HashMap<>();
    }

    /**
     * Retrieve the connection info for a symbol within the context
     *
     * @return
     */
    public IOTableCell getCellBySymbol(Symbol symbol) {
        return cells.getOrDefault(symbol, null);
    }

    public void addCell(IOTableCell cell) {
        cells.put(cell.fieldSymbol, cell);
    }

    public void removeCell(IOTableCell cell) {
        removeCell(cell.fieldSymbol);
    }

    public void removeCell(Symbol symbol) {
        if (cells.containsKey(symbol))
            cells.remove(symbol);
    }

    /**
     * Get the parent node host the IOTable
     *
     * @return
     */
    public Vertex getContext() {
        return context;
    }

    public List<Symbol> getSymbols() {
        return new ArrayList<>(cells.keySet());
    }

    public List<IOTableCell> getCells() {
        return new ArrayList<>(cells.values());
    }

    public Symbol getSymbolByVarName(String symbolName) {
        for (Symbol symbol : cells.keySet()) {
            if (symbol.getSymbolName().equals(symbolName)) {
                return symbol;
            }
        }
        return null;
    }

    public boolean containsSymbol(Symbol symbol) {
        return cells.containsKey(symbol);
    }
}
