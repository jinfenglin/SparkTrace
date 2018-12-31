package traceability.components.maven;

import traceability.components.abstractComponents.TraceArtifact;

public class MavenImprovement extends TraceArtifact {
    private String issue_id, issue_resolution, issue_content, issue_priority, issue_assignee, issue_assignee_username, issue_summary, issue_status, issue_type;
    private String issue_resolved_date, issue_created_date;


    public String getIssue_resolved_date() {
        return issue_resolved_date;
    }

    public void setIssue_resolved_date(String issue_resolved_date) {
        this.issue_resolved_date = issue_resolved_date;
    }

    public String getIssue_created_date() {
        return issue_created_date;
    }

    public void setIssue_created_date(String issue_created_date) {
        this.issue_created_date = issue_created_date;
    }

    public String getCommit_id() {
        return issue_id;
    }

    public void setIssue_id(String issue_id) {
        this.issue_id = issue_id;
    }

    public String getIssue_resolution() {
        return issue_resolution;
    }

    public void setIssue_resolution(String issue_resolution) {
        this.issue_resolution = issue_resolution;
    }

    public String getIssue_content() {
        return issue_content;
    }

    public void setIssue_content(String issue_content) {
        this.issue_content = issue_content;
    }

    public String getIssue_priority() {
        return issue_priority;
    }

    public void setIssue_priority(String issue_priority) {
        this.issue_priority = issue_priority;
    }

    public String getIssue_assignee() {
        return issue_assignee;
    }

    public void setIssue_assignee(String issue_assignee) {
        this.issue_assignee = issue_assignee;
    }

    public String getIssue_assignee_username() {
        return issue_assignee_username;
    }

    public void setIssue_assignee_username(String issue_assignee_username) {
        this.issue_assignee_username = issue_assignee_username;
    }

    public String getIssue_summary() {
        return issue_summary;
    }

    public void setIssue_summary(String issue_summary) {
        this.issue_summary = issue_summary;
    }

    public String getIssue_status() {
        return issue_status;
    }

    public void setIssue_status(String issue_status) {
        this.issue_status = issue_status;
    }

    public String getIssue_type() {
        return issue_type;
    }

    public void setIssue_type(String issue_type) {
        this.issue_type = issue_type;
    }

}
