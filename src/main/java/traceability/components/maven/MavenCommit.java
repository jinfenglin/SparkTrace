package traceability.components.maven;

import traceability.components.abstractComponents.TraceArtifact;

public class MavenCommit extends TraceArtifact {
    private String commit_id,commit_date,content,author,type;

    public String getCommit_id() {
        return commit_id;
    }

    public void setCommit_id(String commit_id) {
        this.commit_id = commit_id;
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
