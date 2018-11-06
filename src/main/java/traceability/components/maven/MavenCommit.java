package traceability.components.maven;

import traceability.components.abstractComponents.TraceArtifact;

public class MavenCommit extends TraceArtifact {
    private String id,commit_date,content,author,type;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCommit_date() {
        return commit_date;
    }

    public void setCommit_date(String commit_date) {
        this.commit_date = commit_date;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
