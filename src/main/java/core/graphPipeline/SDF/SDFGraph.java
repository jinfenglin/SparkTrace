package core.graphPipeline.SDF;

import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import core.graphPipeline.basic.SGraph;

import java.util.*;


public class SDFGraph extends SGraph implements SDFInterface {
    public enum SDFType {
        SOURCE_SDF,
        TARGET_SDF,
    }

    private Map<String, SDFType> outputTypeMap;

    public SDFGraph() {
        super();
        outputTypeMap = new HashMap<>();
    }

    public SDFGraph(String id) {
        super(id);
        outputTypeMap = new HashMap<>();
    }

    public SDFGraph(SGraph sGraph, Map<String, SDFType> outputTypeMap) {
        super(sGraph);
        this.outputTypeMap = outputTypeMap;
    }

    @Override
    public void removeOutputField(Symbol symbol) {
        super.removeOutputField(symbol);
        outputTypeMap.remove(symbol.getSymbolName());
    }

    @Override
    public Map<String, SDFType> getOutputTypeMap() {
        return outputTypeMap;
    }

    private Set<String> getSymbolValuesByName(Set<String> names) {
        Set<String> symbolValues = new HashSet<>();
        for (String name : names) {
            String value = SymbolTable.getSymbolValue(new Symbol(this, name));
            symbolValues.add(value);
        }
        return symbolValues;
    }

    @Override
    public Set<String> getTargetSDFOutputs() {
        Set<String> symbolNames = splitSDFOutputs().getValue();
        return getSymbolValuesByName(symbolNames);
    }

    @Override
    public Set<String> getSourceSDFOutput() {
        Set<String> symbolNames = splitSDFOutputs().getKey();
        return getSymbolValuesByName(symbolNames);
    }

    public Vertex addOutputField(Symbol symbol, SDFType type) throws Exception {
        super.addOutputField(symbol);
        assignTypeToOutputField(symbol.getSymbolName(), type);
        return this;
    }

    public Vertex addOutputField(String symbolName, SDFType type) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        addOutputField(symbol, type);
        return this;
    }
}
