package core.GraphSymbol;

import core.pipelineOptimizer.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A global table record the all available symbols.
 */
public final class SymbolTable {
    private static Map<Symbol, String> outputSymbolValues, inputSymbolValues;

    private SymbolTable() {
    }

    public static Map<Symbol, String> getOutputSymbolValueMap() {
        if (outputSymbolValues == null) {
            outputSymbolValues = new HashMap<>();
        }
        return outputSymbolValues;
    }

    public static Map<Symbol, String> getInputSymbolValueMap() {
        if (inputSymbolValues == null) {
            inputSymbolValues = new HashMap<>();
        }
        return inputSymbolValues;
    }

    private static String resolveValueConflicts(Symbol symbol) {
        return symbol.getScope().getVertexId() + "_" + symbol.getSymbolName();
    }

    /**
     * The default symbol value is symbol name
     *
     * @param symbol
     * @return
     */
    private static String defaultSymbolValue(Symbol symbol) {
        return symbol.getSymbolName();
    }

    public static String getInputSymbolValue(Symbol symbol) {
        Map<Symbol, String> symbolValueMap = getInputSymbolValueMap();
        return symbolValueMap.get(symbol);
    }

    public static void setInputSymbolValue(Symbol symbol, String value) {
        Map<Symbol, String> symbolValueMap = getInputSymbolValueMap();
        symbolValueMap.put(symbol, value);
    }

    public static String getOutputSymbolValue(Symbol symbol) {
        Map<Symbol, String> symbolValueMap = getOutputSymbolValueMap();
        return symbolValueMap.get(symbol);
    }

    public static void setOutputSymbolValue(Symbol symbol, String value) {
        Map<Symbol, String> symbolValueMap = getOutputSymbolValueMap();
        symbolValueMap.put(symbol, value);
    }

    public static void registerInputSymbol(Symbol symbol) {
        Map<Symbol, String> symbolValueMap = getInputSymbolValueMap();
        symbolValueMap.put(symbol, defaultSymbolValue(symbol));
    }

    public static void registerOutputSymbol(Symbol symbol) {
        Map<Symbol, String> symbolValueMap = getOutputSymbolValueMap();
        if (!symbolValueMap.containsKey(symbol)) {
            Set<String> valueSet = new HashSet<>(symbolValueMap.values());
            String defaultVal = defaultSymbolValue(symbol);
            if (valueSet.contains(defaultVal)) {
                symbolValueMap.put(symbol, resolveValueConflicts(symbol));
            } else {
                symbolValueMap.put(symbol, defaultVal);
            }
        }
    }

    /**
     * Share the provider's symbol value to receiver. The boolean parameter determines search the provider and receiver
     * symbols in which symbol set. If true, the value is passed from inputTable to outputtable,
     * otherwise it is from output table to inputtable
     *
     * @param valueProvider
     * @param receiver
     */
    public static void shareSymbolValue(Symbol valueProvider, Symbol receiver, boolean shareOutputToInput) throws Exception {
        Map<Symbol, String> inputSymbolValueMap = getInputSymbolValueMap();
        Map<Symbol, String> outputSymbolValueMap = getOutputSymbolValueMap();
        String value = null;
        if (shareOutputToInput) {
            value = outputSymbolValueMap.get(valueProvider);
        } else {
            value = inputSymbolValueMap.get(valueProvider);
        }

        if (value == null) {
            throw new Exception(String.format("valueProvider %s or receiver %s is not found", valueProvider, receiver));
        }
        if (shareOutputToInput) {
            inputSymbolValueMap.put(receiver, value);
        } else {
            outputSymbolValueMap.put(receiver, value);
        }
    }

}
