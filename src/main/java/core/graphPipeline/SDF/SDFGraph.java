package core.graphPipeline.SDF;

import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import core.graphPipeline.basic.SGraph;

import java.util.*;


public class SDFGraph extends SGraph implements SDFInterface {
    //SDFGraph outputFields are generated by sourceSDF and targetSDF logically
    private Map<String, SDFNode.SDFType> outputTypeMap;

    public SDFGraph(){
        super();
        outputTypeMap = new HashMap<>();
    }

    @Override
    public void addNode(Vertex vertex) {
        assert vertex instanceof SDFNode || vertex instanceof SDFGraph;
        super.addNode(vertex);
    }

    @Override
    public void removeNode(Vertex vertex) {
        assert vertex instanceof SDFNode || vertex instanceof SDFGraph;
        super.removeNode(vertex);
    }

    @Override
    public void connectSymbol(Vertex v1, String symbolName1, Vertex v2, String symbolName2) {
        super.connectSymbol(v1, symbolName1, v2, symbolName2);
        if (v2.equals(this.sinkNode)) {
            if (v1 instanceof SDFInterface) {
                SDFInterface sdfV1 = (SDFInterface) v1;
                SDFNode.SDFType sourceSDFType = sdfV1.getOutputSymbolType(symbolName1);
                outputTypeMap.put(symbolName2, sourceSDFType);
            }
        }
    }

    @Override
    public void disconnect(Vertex v1, String symbolName1, Vertex v2, String symbolName2) {
        super.disconnect(v1, symbolName1, v2, symbolName2);
        if (v2.equals(this.sinkNode)) {
            if (v1 instanceof SDFInterface) {
                if (outputTypeMap.containsKey(symbolName2)) {
                    outputTypeMap.remove(symbolName2);
                }
            }
        }
    }

    @Override
    public Map<String, SDFNode.SDFType> getOutputTypeMap() {
        return outputTypeMap;
    }

    private Set<String> getSymbolValuesByName(Set<String> names) {
        Set<String> symbolValues = new HashSet<>();
        for (String name : names) {
            String value = SymbolTable.getOutputSymbolValue(new Symbol(this, name));
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
}
