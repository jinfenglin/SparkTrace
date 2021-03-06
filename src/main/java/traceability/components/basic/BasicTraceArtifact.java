package traceability.components.basic;

import traceability.components.abstractComponents.TraceArtifact;

/**
 * Basic artifact which only have ID and Content field. The POJOs must have default constructor which have no parameters
 * for Spark.
 */
public class BasicTraceArtifact extends TraceArtifact {
    private String id;
    private String content;

    public BasicTraceArtifact() {

    }

    public BasicTraceArtifact(String id) {
        this.id = id;
        this.content = "";
    }

    public BasicTraceArtifact(String id, String content) {
        this.id = id;
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getCommit_id() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "BasicTraceArtifact{" +
                "id='" + id + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
