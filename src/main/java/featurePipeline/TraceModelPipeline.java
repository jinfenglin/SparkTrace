package featurePipeline;

public class TraceModelPipeline {
    private String featureColName;

    public TraceModelPipeline(String featureColName) {
        this.featureColName = featureColName;
    }

    public void setFeatureColName(String featureColName) {
        this.featureColName = featureColName;
    }

    public String getFeatureColName() {
        return featureColName;
    }
}
