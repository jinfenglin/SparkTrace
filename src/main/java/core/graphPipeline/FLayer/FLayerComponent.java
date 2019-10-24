package core.graphPipeline.FLayer;

import org.apache.spark.sql.Dataset;

import java.util.List;

public interface FLayerComponent {
    /**
     * Generate a list of Dataset as results by executing the code defined in this graph component.
     *
     * @return
     */
    List<Dataset> compute();
}
