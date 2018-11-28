package featurePipeline;

import org.apache.spark.ml.Pipeline;

/**
 * Apply VetorAssember to produce feature vectors for the predictionModel
 */
public class DDFPipeline extends Pipeline {
    private String sourceArtifactColName, targetArtifactColName;

    public DDFPipeline(String sourceArtifactColName, String targetArtifactColName) {
        super();
        this.sourceArtifactColName = sourceArtifactColName;
        this.targetArtifactColName = targetArtifactColName;
    }

    public String getSourceArtifactColName() {
        return sourceArtifactColName;
    }

    public String getTargetArtifactColName() {
        return targetArtifactColName;
    }

    public void setSourceArtifactColName(String sourceArtifactColName) {
        this.sourceArtifactColName = sourceArtifactColName;
    }

    public void setTargetArtifactColName(String targetArtifactColName) {
        this.targetArtifactColName = targetArtifactColName;
    }
}
