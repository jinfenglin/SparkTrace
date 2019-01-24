package core.graphPipeline.SDF;

import core.graphPipeline.basic.SNode;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import org.apache.spark.ml.PipelineStage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SDFNode extends SNode implements SDFInterface {
    private Map<String, SDFType> outputTypeMap;

    /**
     * SDFNode are logically classified as 2 types. The source_art_SDF generate features for source artifacts.
     */
    public enum SDFType {
        SOURCE_SDF, //SDFNode for SourceSDF
        TARGET_SDF,
    }

    public SDFNode(PipelineStage pipelineStage) {
        super(pipelineStage);
        outputTypeMap = new HashMap<>();
    }

    public SDFNode(PipelineStage pipelineStage, String nodeId) {
        super(pipelineStage, nodeId);
        outputTypeMap = new HashMap<>();
    }

    @Override
    public Map<String, SDFType> getOutputTypeMap() {
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
        Set<String> symbolNames = splitSDFOutputs().getKey();
        return getSymbolValuesByName(symbolNames);
    }

    @Override
    public Set<String> getSourceSDFOutput() {
        Set<String> symbolNames = splitSDFOutputs().getValue();
        return getSymbolValuesByName(symbolNames);
    }

}
