package traceability.components.abstractComponents;

public abstract class TraceLink extends TraceComponent {
    public TraceLink() {
    }

    public abstract String getSourceArtifactID();

    public abstract String getTargetArtifactID();
}
