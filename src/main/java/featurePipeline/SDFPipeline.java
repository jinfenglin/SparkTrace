package featurePipeline;

/**
 * Single Document Feature pipeline. The stages in this pipeline process one column of the dataset and generate one or
 * more columns of data.
 */
public class SDFPipeline {
    private String idColName;

    public SDFPipeline(String idColName) {
        super();
        this.idColName = idColName;
    }

    public String getIdColName() {
        return idColName;
    }

}
