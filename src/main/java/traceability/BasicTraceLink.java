package traceability;

public class BasicTraceLink extends TraceLink {
    private String sourceArtifactID, targetArtifactID;
    private String label;

    public BasicTraceLink() {

    }

    public BasicTraceLink(String sourceArtifactID, String targetArtifactID, String label) {
        this.sourceArtifactID = sourceArtifactID;
        this.targetArtifactID = targetArtifactID;
        this.label = label;
    }

    public String getSourceArtifactID() {
        return sourceArtifactID;
    }

    public String getTargetArtifactID() {
        return targetArtifactID;
    }

    public void setTargetArtifactID(String targetArtifactID) {
        this.targetArtifactID = targetArtifactID;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setSourceArtifactID(String sourceArtifactID) {
        this.sourceArtifactID = sourceArtifactID;

    }
}
