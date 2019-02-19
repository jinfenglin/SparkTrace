package core.graphPipeline.graphSymbol;

import java.util.*;

/**
 * A global table record the all available symbols.
 */
public final class SymbolTable {

    private SymbolTable() {
    }

    private static Map<Symbol, String> getSymbolValueMap(Symbol symbol) {
        return symbol.getScope().getSymbolValues();
    }

    /**
     * The default symbol value is symbol name
     *
     * @param symbol
     * @return
     */
    private static String defaultSymbolValue(Symbol symbol) {
        return String.format("%s_%s_%s", symbol.getScope().getVertexId(), symbol.getSymbolName(), UUID.randomUUID().toString().replace("-", "_"));
    }


    public static String getSymbolValue(Symbol symbol) {
        assert symbol.getScope().getSymbolValues().containsKey(symbol);
        Map<Symbol, String> symbolValueMap = getSymbolValueMap(symbol);
        return symbolValueMap.get(symbol);
    }

    public static void setSymbolValue(Symbol symbol, String value) {
        Map<Symbol, String> symbolValueMap = getSymbolValueMap(symbol);
        symbolValueMap.put(symbol, value);
    }

    public static void registerSymbol(Symbol symbol) {
        assert !symbol.getScope().getSymbolValues().containsKey(symbol);
        setSymbolValue(symbol, defaultSymbolValue(symbol));
    }

    /**
     * Share the provider's symbol value to receiver. The boolean parameter determines search the provider and receiver
     * symbols in which symbol set. If true, the value is passed from inputTable to outputtable,
     * otherwise it is from output table to inputtable
     *
     * @param valueProvider
     * @param receiver
     */
    public static void shareSymbolValue(Symbol valueProvider, Symbol receiver) throws Exception {
        String value = getSymbolValue(valueProvider);
        setSymbolValue(receiver, value);
    }

}
