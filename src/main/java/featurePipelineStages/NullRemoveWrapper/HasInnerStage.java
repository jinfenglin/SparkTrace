package featurePipelineStages.NullRemoveWrapper;

import org.apache.spark.ml.PipelineStage;

/**
 * Interface for any Stage wrapper which have inner stage
 */
public interface HasInnerStage {
    /**
     * Get a read-only copy of the inner stage. If user wanna modify the inner stage should use the setInnerStage method
     *
     * @return
     */
    PipelineStage getInnerStage();

    /**
     * Assign a stage as the inner stage
     *
     * @param stage
     */
    void setInnerStage(PipelineStage stage);
}
