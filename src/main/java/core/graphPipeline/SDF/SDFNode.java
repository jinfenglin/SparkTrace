package core.graphPipeline.SDF;

import core.graphPipeline.basic.SNode;
import org.apache.spark.ml.PipelineStage;

/**
 *
 */


public class SDFNode extends SNode {

    /**
     * SDFNode are logically classified as 2 types. The source_art_SDF generate features for source artifacts.
     */
    public enum SDFNodeType {
        SOURCE_ART_SDF, //SDFNode for SourceSDF
        TARGET_ART_SDF
    }

    private SDFNodeType type;

    public SDFNode(PipelineStage pipelineStage, SDFNodeType type) {
        super(pipelineStage);
        this.type = type;
    }

    public SDFNode(PipelineStage pipelineStage, String nodeId, SDFNodeType type) {
        super(pipelineStage, nodeId);
        this.type = type;
    }

    public SDFNodeType getType() {
        return type;
    }

    public void setType(SDFNodeType type) {
        this.type = type;
    }
}
