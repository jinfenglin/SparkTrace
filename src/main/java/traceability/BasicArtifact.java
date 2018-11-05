package traceability;

public class BasicArtifact extends Artifact{
    private String id;

    public BasicArtifact(){

    }

    public BasicArtifact(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
