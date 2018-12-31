package traceability.components.maven;

import traceability.components.abstractComponents.TraceArtifact;

public class MavenCommit extends TraceArtifact {
    private String commit_id,commit_date, commit_content, commit_author, commit_type;

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

    public String getCommit_content() {
        return commit_content;
    }

    public void setCommit_content(String commit_content) {
        this.commit_content = commit_content;
    }

    public String getCommit_author() {
        return commit_author;
    }

    public void setCommit_author(String commit_author) {
        this.commit_author = commit_author;
    }

    public String getCommit_type() {
        return commit_type;
    }

    public void setCommit_type(String commit_type) {
        this.commit_type = commit_type;
    }
}
