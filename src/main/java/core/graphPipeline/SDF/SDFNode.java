package core.graphPipeline.SDF;

import core.graphPipeline.basic.SNode;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import org.apache.spark.ml.PipelineStage;

import java.util.HashSet;
import java.util.Set;

public class SDFNode extends SNode {
    private Set<String> sourceOutputCols, targetOutputCols;

    /**
     * SDFNode are logically classified as 2 types. The source_art_SDF generate features for source artifacts.
     */
    public enum SDFType {
        SOURCE_ART_SDF, //SDFNode for SourceSDF
        TARGET_ART_SDF,
        SHARED_SDF; // A shared Node for source and target SDF
    }

    private SDFType type;

    public SDFNode(PipelineStage pipelineStage, SDFType type) {
        super(pipelineStage);
        this.type = type;
        sourceOutputCols = new HashSet<>();
        targetOutputCols = new HashSet<>();
    }

    public SDFNode(PipelineStage pipelineStage, String nodeId, SDFType type) {
        super(pipelineStage, nodeId);

        this.type = type;
        sourceOutputCols = new HashSet<>();
        targetOutputCols = new HashSet<>();
    }

    /**
     * Get SDFType for a given symbol.
     *
     * @param symbol
     * @return
     */
    public SDFType getOutputSymbolType(Symbol symbol) {
        return type;
    }

    public SDFType getType() {
        return type;
    }

    public void setType(SDFType type) {
        this.type = type;
    }

    /**
     * Node with SOURCE_SDF or TARGET_SDF type can assign the added Ouputfiled to a type automatically
     *
     * @param symbolName
     * @return
     * @throws Exception
     */
    @Override
    public Vertex addOutputField(String symbolName) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        return addOutputField(symbol, getType());
    }

    @Override
    public Vertex addOutputField(Symbol symbol) throws Exception {
        return addOutputField(symbol, getType());
    }

    /**
     * A SHARED_SDF must use this method to add output fields
     *
     * @param symbol
     * @param type
     * @return
     * @throws Exception
     */
    public Vertex addOutputField(Symbol symbol, SDFType type) throws Exception {
        if (type.equals(SDFType.SOURCE_ART_SDF)) {
            sourceOutputCols.add(symbol.getSymbolName());
        } else if (type.equals(SDFType.TARGET_ART_SDF)) {
            targetOutputCols.add(symbol.getSymbolName());
        } else {
            throw new Exception("SDF %s with type %s can not process outputField symbol %s, user should specify symbol %s SDFType explicitly");
        }
        return super.addOutputField(symbol);
    }

    public Vertex addOutputField(String symbolName, SDFType type) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        return addOutputField(symbol, type);
    }

}
