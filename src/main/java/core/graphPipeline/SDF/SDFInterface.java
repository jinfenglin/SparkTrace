package core.graphPipeline.SDF;

import core.graphPipeline.graphSymbol.Symbol;
import javafx.util.Pair;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public interface SDFInterface {
    default SDFNode.SDFType getOutputSymbolType(String symbolName) {
        return getOutputTypeMap().get(symbolName);
    }

    Map<String, SDFNode.SDFType> getOutputTypeMap();

    default void assignTypeToOutputField(String symbolName, SDFNode.SDFType type) {
        getOutputTypeMap().put(symbolName, type);
    }

    /**
     * Split the sdf output as sourceSDF and targetSDF
     *
     * @return
     */
    default Pair<Set<String>, Set<String>> splitSDFOutputs() {
        Map<String, SDFNode.SDFType> outputTypes = getOutputTypeMap();
        Set<String> sourceSymbol = new HashSet<>();
        Set<String> targetSymbol = new HashSet<>();
        for (String outputField : outputTypes.keySet()) {
            SDFNode.SDFType type = outputTypes.get(outputField);
            if (type.equals(SDFNode.SDFType.SOURCE_SDF)) {
                sourceSymbol.add(outputField);
            } else {
                targetSymbol.add(outputField);
            }
        }
        return new Pair<>(sourceSymbol, targetSymbol);
    }

    /**
     * Get the output filed symbol values for the target output fileds
     *
     * @return
     */
    Set<String> getTargetSDFOutputs();

    Set<String> getSourceSDFOutput();
}
