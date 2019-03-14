package traceability.components.maven;

import traceability.components.abstractComponents.TraceLink;

public class MavenICLink extends TraceLink {
    String issue_id, commit_id, method, score;

    public String getIssue_id() {
        return issue_id;
    }

    public void setIssue_id(String issue_id) {
        this.issue_id = issue_id;
    }

    public String getCommit_id() {
        return commit_id;
    }

    public void setCommit_id(String commit_id) {
        this.commit_id = commit_id;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    @Override
    public String getSourceArtifactID() {
        return getIssue_id();
    }

    @Override
    public String getTargetArtifactID() {
        return getCommit_id();
    }
}
