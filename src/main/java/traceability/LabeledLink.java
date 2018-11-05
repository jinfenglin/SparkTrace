package traceability;

public class LabeledLink extends Link {
    private String label;

    public LabeledLink(Artifact from, Artifact to, String label) {
        super(from, to);
        this.label = label;
    }
}
