package core.graphPipeline.basic;

import core.graphPipeline.graphSymbol.Symbol;

import java.util.*;

/**
 * IOTable recorded the mapping relationships between two SNode. The IOTableCells will connect to another IOTableCell.In
 * each SNode there will be one input table and one output table;
 */
public class IOTable {
    private Vertex context; //IOTable must reside within a vertex
    private Map<Symbol, IOTableCell> cells; // A HashMap for fast access to IOTableCell

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

    public void addCell(IOTableCell cell) throws Exception {
        String errorMessage = "It is not valid to add cell %s to vertex %s because the context is different";
        if (cell.getFieldSymbol().getScope() != context)
            throw new Exception(String.format(errorMessage, cell.toString(), context.toString()));
        cell.setParentTable(this);
        cells.put(cell.getFieldSymbol(), cell);
    }

    public void removeCell(IOTableCell cell) {
        removeCell(cell.getFieldSymbol());
    }

    public void removeCell(Symbol symbol) {
        if (cells.containsKey(symbol)) {
            cells.get(symbol).setParentTable(null);
            cells.remove(symbol);
        }
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

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner("\n");
        for (Symbol symbol : cells.keySet()) {
            IOTableCell cell = cells.get(symbol);
            joiner.add(cell.toString());
        }
        return joiner.toString();
    }

    public enum TableType {
        INPUT_TABLE, OUTPUT_TABLE;
    }
}
