package traceability;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Artifact implements Serializable {
    private String artifactID;
    private Map<String, String> attributes;


    public Artifact(String artifactID) {
        this.artifactID = artifactID;
        this.attributes = new HashMap<>();
    }

    public void addAttribute(String attribName, String value) {
        attributes.put(attribName, value);
    }

    public String getArtifactID() {
        return artifactID;
    }

    public void setArtifactID(String artifactID) {
        this.artifactID = artifactID;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Artifact{" +
                "artifactID='" + artifactID + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}
