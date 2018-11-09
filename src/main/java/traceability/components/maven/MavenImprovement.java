package traceability.components.maven;

import traceability.components.abstractComponents.TraceArtifact;

public class MavenImprovement extends TraceArtifact {
    private String issue_id, resolution, content, priority, assignee, assignee_username, summary, status, type;
    private String resolved_date, created_date;


    public String getResolved_date() {
        return resolved_date;
    }

    public void setResolved_date(String resolved_date) {
        this.resolved_date = resolved_date;
    }

    public String getCreated_date() {
        return created_date;
    }

    public void setCreated_date(String created_date) {
        this.created_date = created_date;
    }

    public String getCommit_id() {
        return issue_id;
    }

    public void setIssue_id(String issue_id) {
        this.issue_id = issue_id;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getAssignee() {
        return assignee;
    }

    public void setAssignee(String assignee) {
        this.assignee = assignee;
    }

    public String getAssignee_username() {
        return assignee_username;
    }

    public void setAssignee_username(String assignee_username) {
        this.assignee_username = assignee_username;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
